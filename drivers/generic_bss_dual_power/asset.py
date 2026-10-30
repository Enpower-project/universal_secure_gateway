"""Expose a simulated battery with separate charge/discharge channels on NATS.

The driver wraps ``bss.BSS`` with fixed defaults: an initially empty 110,000 Wh
battery, 16,000 W charge and discharge limits, and 0.95 charge and discharge
efficiencies. Device configuration is not read by this driver. The model starts
at zero power and advances once per polling interval, using that interval in
seconds as its timestep. Local asyncio timers drive updates; shared ``clock``
messages are not consumed. Execution delays do not increase the timestep.

Telemetry is UTF-8 JSON on ``<ASSET_ID>.telemetry.<channel>`` for channels
``charge_power`` (W), ``discharge_power`` (W), and ``state_of_charge`` (%).
Each message contains ``aid`` (asset ID), ``ts`` (current UTC Unix microseconds),
``k`` (channel), ``v`` (numeric value), and ``u`` (unit). Power channels are
nonnegative magnitudes; at most one is nonzero. State of charge uses a 0-100
scale. All three reports from one update share the same timestamp.

Send controls to the exact subject ``<ASSET_ID>.control`` as JSON with ``k``
and ``v`` fields. For example, ``{"k": "charge_power", "v": 1000.0}``
requests charging at 1000 W. ``discharge_power`` requests discharge. Both
commands use the absolute value of ``v``; zero requests idle. Commands replace
the single model power setting rather than accumulating separate setpoints.
The model applies power and energy limits; other control keys are ignored.

NATS connects to ``nats://natsio:4222``. Logs are published as JSON on
``logs.<ASSET_NAME>`` through ``utils.nats_logger``.

When launched as a script, the process manager supplies:

* ``ASSET_ID`` and ``ASSET_NAME``: subject prefix and logger name.
* ``IF_CONFIG``: JSON object with ``url``, ``user``, and ``password`` keys.
  Construction requires these keys, but the simulation makes no device requests.
* ``POLLING_INTERVAL``: integer-compatible reporting period and simulation
  timestep in seconds; configure a positive value. The first report follows
  one interval of sleep.
* ``TOPICS``: valid JSON, stored but not used to select NATS subjects.
* ``ROOT_TOPIC``: stored but not used in NATS subjects.

Missing configuration keys and malformed environment JSON fail startup.
Malformed controls propagate out of queue processing. Status-read errors are
logged and return None, which the telemetry loop does not handle; see
``task_done_callback`` for its limited restart behavior.
"""

import os
import asyncio
from nats.aio.client import Client as NATS
import aiohttp
import json
import msgpack
import uuid
from datetime import datetime, timezone
from bss import BSS

import utils.nats_logger as nats_logger


class Asset:
    """Run a fixed-parameter battery model with NATS telemetry and controls.

    Construction initializes the model. ``run()`` initializes NATS and logging,
    then starts periodic telemetry alongside ordered control processing.
    """

    def __init__(self, id, name, if_config, topics, root_topic, polling_interval):
        """Store process-manager settings and create the battery simulation.

        ``if_config`` must contain url, user, and password. These fields,
        ``topics``, and ``root_topic`` are retained for driver compatibility
        but do not affect simulation or routing. ``polling_interval`` is
        converted to integer seconds. Battery parameters are fixed here.
        """
        self.id = id
        self.name = name
        self.update_interval = int(polling_interval)
        self.if_url = if_config['url']
        self.if_usr = if_config['user']
        self.if_pwd = if_config['password']
        self.topics = topics
        self.root_topic = root_topic
        self.mbus = None
        self.logger = None
        self.queue = asyncio.Queue()

        self.bss=BSS(soc=0.0, capacity=110000.0, max_charge_power=16000.0, max_discharge_power=16000.0, charge_efficiency=0.95, discharge_efficiency=0.95)
    

    async def read_asset_status(self):
        """Advance the model and return asset_id, timestamp, and channel data.

        Convert negative model power to a positive charge_power magnitude and
        positive model power to discharge_power; the other channel is zero.
        State of charge is converted from a fraction to percent. The timestamp
        is current UTC Unix time in microseconds.

        Every call mutates the model by one update interval. Errors are logged
        and return None; logging must already be initialized by ``run()``.
        """
        try:
            # update the asset status
            self.bss.update_bss_state(dt=self.update_interval)

            soc = self.bss.get_soc()*100.0
            power = self.bss.get_power()
            if 0.0 >= power:
                discharge_power = 0.0
                charge_power = abs(power)
            else:
                discharge_power = abs(power)
                charge_power = 0.0

            now = datetime.now(timezone.utc)
            dt = int(now.timestamp() * 1_000_000)

            return {
                # "asset_id": uuid.UUID(self.id).bytes,
                "asset_id": self.id,
                "timestamp": dt,
                "data": [
                    {"k": "charge_power", "v": charge_power, "u": "W"},
                    {"k": "discharge_power", "v": discharge_power, "u": "W"},
                    {"k": "state_of_charge", "v": soc, "u": "%"}
                ]
            }
        
        except Exception as e:
            self.logger.exception(f"Unexpected error in read_asset_status: {e}")

        return None

                
    async def set_power(self, power):
        """Pass a signed power request in watts to the BSS model.

        Negative values charge, positive values discharge, and zero idles.
        The model applies its limits; energy changes on the next status read.
        """
        self.bss.set_power(power)

    
    async def asset_status_loop(self):
        """Sleep, advance the model, and publish each telemetry channel as JSON.

        A status read returning None or a publish error terminates this task.
        """
        while True:
            await asyncio.sleep(self.update_interval)
            self.logger.debug("Get current status")
            status = await self.read_asset_status()
            self.logger.debug(f"Asset status: {status}")
            # repack telemetry into separate messages (individual channel reports)
            for ch_report in status.get('data'):
                data = {
                    "aid": status['asset_id'],
                    "ts": status['timestamp'],
                    "k": ch_report['k'],
                    "v": ch_report['v'],
                    "u": ch_report['u']
                }
                # payload = msgpack.packb(status, use_bin_type=True)
                payload = json.dumps(data).encode()

                # publish status data to the telemetry channel
                await self.mbus.publish(f"{self.id}.telemetry.{data.get('k')}", payload)
    

    def task_done_callback(self, task):
        """Log a failed telemetry task and start one replacement after an error.

        Cancellation is logged without restarting. The replacement does not
        receive this callback, so subsequent failures do not trigger a restart.
        """
        try:
            exception = task.exception()
            if exception:
                self.logger.error(f"Background task crashed: {exception}")
                self.asset_status_task = asyncio.create_task(self.asset_status_loop())
        except asyncio.CancelledError:
            self.logger.info("Background task was cancelled")


    async def process_queue(self):
        """Apply queued charge_power and discharge_power commands in order.

        Decode JSON containing ``k`` and ``v``; use -abs(v) for charging and
        abs(v) for discharging. Other keys have no effect. Invalid JSON,
        missing fields, nonnumeric power, and model errors propagate to run().
        """
        while True:
            msg = await self.queue.get()
            subject = msg.subject
            data = json.loads(msg.data.decode("utf-8"))
            self.logger.debug(f"{subject}: {data}")

            key = data['k']
            value = data['v']
            
            if "charge_power" == key:
                await self.set_power(-abs(value))
            elif "discharge_power" == key:
                await self.set_power(abs(value))

    
    async def msg_handler(self, msg):
        """Queue a NATS control message for processing outside the callback."""
        await self.queue.put(msg)


    async def run(self):
        """Connect to NATS, initialize NATS logging, and run the asset.

        Subscribe to the exact ``<id>.control`` subject, start the initial
        telemetry task with its completion callback, and consume controls until
        cancelled or an error propagates. No explicit NATS drain or background
        task cleanup is provided here.
        """
        self.mbus = NATS()
        await self.mbus.connect("nats://natsio:4222")

        # setup logging over NATS
        nats_logger.init(self.mbus)
        self.logger = nats_logger.get_logger(self.name)

        # subscribe to all messages
        await self.mbus.subscribe(f"{self.id}.control", cb=self.msg_handler)
        # await self.mbus.subscribe("*.control", cb=self.msg_handler)
        
        self.logger.debug(f"subscribed to: {self.id}.control")

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
    polling_interval = os.getenv("POLLING_INTERVAL", None)

    print(asset_id)
    print(asset_name)
    print(asset_interface_config)
    print(topics)
    print(polling_interval)

    asset = Asset(id=asset_id, name=asset_name, if_config=asset_interface_config, topics=topics, root_topic=root_topic, polling_interval=polling_interval)
    asyncio.run(asset.run())


