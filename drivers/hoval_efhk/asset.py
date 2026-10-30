"""Read Hoval EFHK water-heater status over Modbus TCP and publish it to NATS.

The driver polls after a one-second sleep on each iteration. Every status read
connects to the Modbus device, reads 74 input registers starting at address 300
with slave=1, and closes the connection afterward. The polling period therefore
includes device I/O time in addition to the sleep.

Telemetry is one MessagePack snapshot on ``<ASSET_ID>.telemetry`` containing
``asset_id`` (16 UUID bytes), ``timestamp`` (current UTC Unix microseconds),
and ``data`` (a list of dictionaries with ``k``, ``v``, and ``u`` fields):

* ``temperature``: sensor 0 temperature, in degrees Celsius (unit ``°C``).
* ``heater_step``: requested heater step, using unit ``int``.
* ``heater_load``: heater load in watts (unit ``W``).

NATS connects to ``nats://natsio:4222``. Logs are JSON messages on
``logs.<ASSET_NAME>`` through ``utils.nats_logger``. No NATS subscriptions are
enabled in run(), and queued messages are only logged. The heater-step write
helper is available to direct callers but is not connected to NATS controls.

When launched as a script, the process manager supplies:

* ``ASSET_ID``: a UUID string, converted to bytes for telemetry.
* ``ASSET_NAME``: logger name.
* ``IF_CONFIG``: JSON object with ``ip`` and ``port`` for Modbus TCP, plus
  ``url``, ``user``, and ``password`` keys. The latter three are stored but
  unused by device operations.
* ``TOPICS``: valid JSON, stored but not used for NATS routing.
* ``ROOT_TOPIC``: stored but not used in NATS subjects.

Device addresses and float decoding describe this implementation's register
mapping. Read helpers use the pymodbus ``slave`` keyword; the write helper uses
``device_id``. Compatibility depends on the installed client API. Responses
are accessed directly without checking isError(). A status read returning None
is published as MessagePack nil; connection or other unhandled failures can
terminate the telemetry task. See individual methods for error handling.
"""

import os
import asyncio
from nats.aio.client import Client as NATS
import aiohttp
import json
import msgpack
import uuid
from datetime import datetime, timezone
from pymodbus.client import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException
import struct

import utils.nats_logger as nats_logger


class Asset:
    """Manage the Modbus client, NATS telemetry task, and message queue.

    Construction stores configuration only. Call run() to initialize the
    clients and logger before reading or publishing device status.
    """

    def __init__(self, id, name, if_config, topics, root_topic):
        """Store asset identity and required interface configuration keys.

        if_config must provide url, ip, port, user, and password. Only ip and
        port select the device connection. topics and root_topic are retained
        for compatibility but do not determine publication subjects.
        """
        self.id = id
        self.name = name
        self.if_url = if_config['url']
        self.if_ip = if_config['ip']
        self.if_port = if_config['port']
        self.if_usr = if_config['user']
        self.if_pwd = if_config['password']
        self.topics = topics
        self.root_topic = root_topic
        self.modbus_client = None
        self.mbus = None
        self.logger = None
        self.queue = asyncio.Queue()
    

    async def read_water_temperature(self):
        """Read sensor temperature from input registers 325-326, slave 1.

        Decode two words as a big-endian IEEE 754 float in degrees Celsius.
        Requires an initialized client and logger; this helper does not manage
        the connection. Handled errors are logged and implicitly return None.
        The generic error handler references response, so failures before its
        assignment can instead raise UnboundLocalError.
        """
        try:
            response = await self.modbus_client.read_input_registers(address=325, count=2, slave=1)

            temperature = (response.registers[0] << 16) | response.registers[1]
            bytes_value = temperature.to_bytes(4, byteorder='big')
            temperature = struct.unpack('>f', bytes_value)[0] 

            return temperature
        
        except ModbusException as me:
            self.logger.debug(f"Modbus exception: {me}")
        except Exception as e:
            self.logger.debug(f"Exception: {e, response}")
    

    async def read_heater_step(self):
        """Read one holding register at address 200, slave 1, as heater step.

        Three heating elements provide eight settings, numbered 0-7. Return
        the raw register value without range validation. This helper does not
        manage the connection. Handled errors are logged and return None;
        a failure before response assignment can raise UnboundLocalError in
        the generic error handler.
        """
        try:
            response = await self.modbus_client.read_holding_registers(address=200, count=1, slave=1)
            heater_step = response.registers[0]

            return heater_step
        
        except ModbusException as me:
            self.logger.debug(f"Modbus exception: {me}")
        except Exception as e:
            self.logger.debug(f"Exception: {e, response}")


    async def read_em_block(self):
        """Read 74 input registers from address 300, slave 1, into a dictionary.

        Decode the device ID from the first 16 words as UTF-8 byte pairs with
        surrounding NULs removed. Return MODBUS_EMA_* entries for status, load,
        heater step, setpoint, feed-in, emergency mode, heat-pump request,
        analog input, and temperature sensors 0-4. Float fields use two words
        in big-endian order. Registers beyond the decoded fields are unused.

        Requires a connected client and initialized logger. Handled errors
        are logged and return None; a failure before response assignment can
        raise UnboundLocalError in the generic error handler.
        """
        try:
            response = await self.modbus_client.read_input_registers(address=300, count=74, slave=1)

            data = response.registers
            em_block = {}

            # MODBUS_EMA_ID
            strings = []
            id = None
            for item in data[0:16]:
                bytes_data = struct.pack('>h', item)
                # char_str = bytes_data.decode('latin1')
                strings.append(bytes_data.decode('utf-8'))
            id = "".join(strings)
            id = id.strip('\x00')
            em_block['MODBUS_EMA_ID'] = id
            # MODBUS_EMA_STATUS
            em_block['MODBUS_EMA_STATUS'] = data[16]
            # MODBUS_EMA_HEATER_LOAD
            em_block['MODBUS_EMA_HEATER_LOAD'] = data[17]
            # MODBUS_EMA_SET_HEATER_STEP
            em_block['MODBUS_EMA_SET_HEATER_STEP'] = data[18]
            # MODBUS_EMA_LOAD_SETPOINT_VALUE
            em_block['MODBUS_EMA_LOAD_SETPOINT_VALUE'] = data[19]
            # MODBUS_EMA_LOAD_FEEDIN_VALUE
            em_block['MODBUS_EMA_LOAD_FEEDIN_VALUE'] = data[20]
            # MODBUS_EMA_EMERGENCY_MODE
            em_block['MODBUS_EMA_EMERGENCY_MODE'] = data[21]
            # MODBUS_EMA_HEAT_PUMP_REQUEST
            em_block['MODBUS_EMA_HEAT_PUMP_REQUEST'] = data[22]
            # MODBUS_EMA_ANALOG_INPUT_FLOAT
            em_block['MODBUS_EMA_ANALOG_INPUT_FLOAT'] = self.convert_words_to_float(data[23:25])
            # MODBUS_EMA_TEMPERATURE_FLOAT_SENSOR0
            em_block['MODBUS_EMA_TEMPERATURE_FLOAT_SENSOR0'] = self.convert_words_to_float(data[25:27])
            # MODBUS_EMA_TEMPERATURE_FLOAT_SENSOR1
            em_block['MODBUS_EMA_TEMPERATURE_FLOAT_SENSOR1'] = self.convert_words_to_float(data[27:29])
            # MODBUS_EMA_TEMPERATURE_FLOAT_SENSOR2
            em_block['MODBUS_EMA_TEMPERATURE_FLOAT_SENSOR2'] = self.convert_words_to_float(data[29:31])
            # MODBUS_EMA_TEMPERATURE_FLOAT_SENSOR3
            em_block['MODBUS_EMA_TEMPERATURE_FLOAT_SENSOR3'] = self.convert_words_to_float(data[31:33])
            # MODBUS_EMA_TEMPERATURE_FLOAT_SENSOR4
            em_block['MODBUS_EMA_TEMPERATURE_FLOAT_SENSOR4'] = self.convert_words_to_float(data[33:35])

            return em_block
        
        except ModbusException as me:
            self.logger.debug(f"Modbus exception: {me}")
        except Exception as e:
            self.logger.debug(f"Exception: {e, response}")


    async def read_asset_status(self):
        """Connect, read the energy-manager block, and build a telemetry snapshot.

        Return asset_id as UUID bytes, timestamp as UTC Unix microseconds, and
        data for sensor 0 temperature, requested heater step, and heater load.
        The step comes from input register 318 in this block, rather than the
        separate holding-register read at address 200.

        Read/packing errors inside the inner try are logged and return None.
        Connection errors propagate. The client is closed in finally on every
        attempt; the connect() return value is not checked.
        """

        try:
            await self.modbus_client.connect()
            self.logger.debug("Connected to Modbus device")

            try:
                em_block = await self.read_em_block()

                now = datetime.now(timezone.utc)
                dt = int(now.timestamp() * 1_000_000)

                return {
                        "asset_id": uuid.UUID(self.id).bytes,
                        "timestamp": dt,
                        "data": [
                            {"k": "temperature", "v": em_block.get('MODBUS_EMA_TEMPERATURE_FLOAT_SENSOR0'), "u": "°C"},
                            {"k": "heater_step", "v": em_block.get('MODBUS_EMA_SET_HEATER_STEP'), "u": "int"},
                            {"k": "heater_load", "v": em_block.get('MODBUS_EMA_HEATER_LOAD'), "u": "W"}
                        ]
                    }

            except ModbusException as me:
                self.logger.debug(f"Modbus exception: {me}")
            except Exception as e:
                self.logger.debug(f"Unexpected error: {e}")

        finally:
            self.modbus_client.close()
            self.logger.debug("Disconnected from Modbus TCP device")

        return None

                
    async def set_heater_step(self, heater_step):
        """Write a requested heater step to register 318 using device_id=1.

        Expected settings are integers 0-7, but no validation is performed.
        This helper neither connects nor closes the client and is not invoked
        by the message queue. Exceptions are logged; the write response is not
        checked and no success value is returned.
        """
        try:
            await self.modbus_client.write_register(address=318, value=heater_step, device_id=1) 
        
        except ModbusException as me:
            self.logger.debug(f"Modbus exception: {me}")
        except Exception as e:
            self.logger.debug(f"Exception: {e}")
    

    def convert_words_to_float(self, words_list):
        """Decode two 16-bit words, high word first, as a big-endian float32.

        Missing or invalid words raise the underlying indexing, conversion,
        or struct error. Additional list entries are ignored.
        """
        # convert a list of two words into floating point value
        value = (words_list[0] << 16) | words_list[1]
        bytes_value = value.to_bytes(4, byteorder='big')
        value = struct.unpack('>f', bytes_value)[0]

        return value

    
    async def asset_status_loop(self):
        """Poll after each one-second sleep and publish a MessagePack snapshot.

        Publish None as MessagePack nil when the reader returns None. Unhandled
        read, serialization, or NATS publish errors terminate this task.
        """
        while True:
            await asyncio.sleep(1)
            self.logger.debug("Get current status")
            status = await self.read_asset_status()
            self.logger.debug(f"Asset status: {status}")
            payload = msgpack.packb(status, use_bin_type=True)

            # publish status data to the telemetry channel
            await self.mbus.publish(f"{self.id}.telemetry", payload)
    

    def task_done_callback(self, task):
        """Log telemetry failure and start one replacement task after an error.

        Cancellation is logged without restarting. The replacement does not
        receive this callback, so later failures do not trigger another restart.
        """
        try:
            exception = task.exception()
            if exception:
                self.logger.error(f"Background task crashed: {exception}")
                self.asset_status_task = asyncio.create_task(self.asset_status_loop())
        except asyncio.CancelledError:
            self.logger.info("Background task was cancelled")


    async def process_queue(self):
        """Consume queued messages and log their subject and decoded payload.

        No commands are dispatched. With the subscriptions in run() disabled,
        this waits indefinitely unless a caller explicitly enqueues messages.
        Payload decoding errors propagate.
        """
        while True:
            msg = await self.queue.get()
            self.logger.debug(f"{msg.subject}: {msg.data.decode()}")

    
    async def msg_handler(self, msg):
        """Enqueue a NATS message for logging outside the callback."""
        await self.queue.put(msg)


    async def run(self):
        """Initialize NATS logging and Modbus, then run telemetry and the queue.

        Attach a completion callback to the initial telemetry task. No NATS
        subscriptions are enabled. This coroutine awaits queue processing
        without explicitly draining NATS or cancelling telemetry on exit.
        """
        self.mbus = NATS()
        await self.mbus.connect("nats://natsio:4222")

        # setup logging over NATS
        nats_logger.init(self.mbus)
        self.logger = nats_logger.get_logger(self.name)

        # subscribe to all messages
        # await self.mbus.subscribe(f">", cb=self.msg_handler)

        # setup Modbus client
        self.modbus_client = AsyncModbusTcpClient(self.if_ip, port=self.if_port)

        # start loop for periodic asset telemetry retrieval 
        self.asset_status_task = asyncio.create_task(self.asset_status_loop())
        self.asset_status_task.add_done_callback(self.task_done_callback)

        await self.process_queue()

        # while True:
        #     await asyncio.sleep(1)

    

if '__main__' == __name__:
    asset_id = os.getenv("ASSET_ID", None)
    asset_name = os.getenv("ASSET_NAME", None)
    asset_interface_config = json.loads(os.getenv("IF_CONFIG", None))
    topics = json.loads(os.getenv("TOPICS", None))
    root_topic = os.getenv("ROOT_TOPIC", None)

    print(asset_id)
    print(asset_name)
    print(asset_interface_config)
    print(topics)

    asset = Asset(id=asset_id, name=asset_name, if_config=asset_interface_config, topics=topics, root_topic=root_topic)
    asyncio.run(asset.run())

