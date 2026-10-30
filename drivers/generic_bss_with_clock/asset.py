"""Simulate a battery and publish telemetry when NATS clock updates are due.

The driver subscribes to ``clock`` and ``<ASSET_ID>.control.*`` on
``nats://natsio:4222``. Clock messages are UTF-8 JSON containing an ISO 8601
``timestamp``, for example ``{"timestamp": "2026-03-12T12:40:00"}``.
Use consistently naive timestamps or consistently timezone-aware timestamps;
the driver parses them with datetime.fromisoformat without normalizing them.

Telemetry is UTF-8 JSON on ``<ASSET_ID>.telemetry.active_power`` and
``<ASSET_ID>.telemetry.state_of_charge``. Each message contains ``aid`` (asset
ID), ``ts`` (wall-clock UTC Unix time in microseconds, not simulation time),
``k`` (channel), ``v`` (numeric value), and ``u`` (``W`` or ``%``). Both channel
reports share a snapshot timestamp. Positive power means discharge; negative
power means charge. State of charge is reported on a 0-100 scale.

Controls contain ``k`` and ``v`` fields, for example
``{"k": "active_power", "v": -1000.0}`` to request charging at 1000 W.
The payload key selects the action independently of the subject suffix.
Other control keys are ignored. Controls and clocks are processed in queue
order; the current power setting is used for the next model update.

When launched as a script, the process manager supplies:

* ``ASSET_ID`` and ``ASSET_NAME``: telemetry subject prefix and logger name.
* ``IF_CONFIG``: JSON object containing ``url``, ``user``, and ``password``.
  These keys are required, but the simulation makes no device requests.
* ``DEV_CONFIG``: JSON object containing ``soc`` (initial fraction, 0-1),
  ``capacity`` (Wh), ``max_charge_power`` and ``max_discharge_power`` (W),
  and ``charge_efficiency`` and ``discharge_efficiency`` (fractions).
* ``POLLING_INTERVAL``: integer-compatible update threshold and model timestep
  in seconds; configure a positive value.
* ``TOPICS``: valid JSON, stored but not used for NATS routing.
* ``ROOT_TOPIC``: stored but not used in NATS subjects.

Missing configuration keys and invalid environment JSON fail startup. Invalid
clock/control messages and publish errors propagate out of queue processing.
Status-read errors are logged and return None, which the publisher does not
handle. No telemetry background task or automatic restart is enabled in run().
"""

import os
import asyncio
from nats.aio.client import Client as NATS
import aiohttp
import json
import msgpack
import uuid
from datetime import datetime, timezone, timedelta
from bss import BSS

# import utils.nats_logger as nats_logger
import logging

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")


class Asset:
    """Coordinate a BSS model, simulation clocks, and NATS power controls.

    Construction initializes the battery at zero power. ``run()`` initializes
    logging and NATS, then processes clock and control messages serially.
    """

    def __init__(self, id, name, if_config, dev_config, topics, root_topic, polling_interval):
        """Build a battery using process-manager configuration dictionaries.

        Pass device parameters directly to BSS and convert polling_interval to
        integer seconds. Interface fields, topics, and root_topic are retained
        for compatibility without affecting simulation or message routing.
        Clock tracking remains unset until the first clock message arrives.
        """
        self.id = id
        self.name = name
        self.if_url = if_config['url']
        self.if_usr = if_config['user']
        self.if_pwd = if_config['password']
        self.topics = topics
        self.root_topic = root_topic
        # clocks and updates
        self.clock = None
        self.previous_clock = None
        # representing a timestamp when the last telemetry was updated
        self.last_update = None
        self.timestep = int(polling_interval)
        self.update_interval = int(polling_interval)
        self.mbus = None
        self.logger = None
        self.queue = asyncio.Queue()

        # init simulated/emulated BSS
        self.bss=BSS(
            soc=dev_config['soc'], 
            capacity=dev_config['capacity'], 
            max_charge_power=dev_config['max_charge_power'], 
            max_discharge_power=dev_config['max_discharge_power'], 
            charge_efficiency=dev_config['charge_efficiency'], 
            discharge_efficiency=dev_config['discharge_efficiency']
        )
    

    def init_clock(self, dt):
        """Set current, previous, and last-update clocks to a datetime.

        This establishes the update baseline without advancing the model.
        Requires the logger initialized by run().
        """
        # set initial clock after receiving clock update event
        self.clock = dt
        self.previous_clock = dt
        self.last_update = dt
        self.logger.debug(f"New clock set: {self.clock}")


    def update_clock(self, clock):
        """Parse an ISO 8601 timestamp and record elapsed clock seconds.

        The first message initializes tracking and produces a zero timestep.
        Later messages update current/previous clocks but preserve last_update.
        No monotonicity check or timezone normalization is performed; mixing
        naive and aware datetimes raises TypeError during subtraction.
        """
        # update the clock datetimes
        dt = datetime.fromisoformat(clock)
        if None == self.clock:
            self.init_clock(dt)
        
        self.previous_clock = self.clock
        self.clock = dt
        self.timestep = (self.clock - self.previous_clock)
        self.timestep = self.timestep.total_seconds()

        self.logger.debug(f"clock: {self.clock}, previous_clock: {self.previous_clock}, timestep: {self.timestep}")


    async def read_asset_status(self):
        """Advance BSS by update_interval seconds and return a snapshot.

        Return asset_id, wall-clock UTC Unix microseconds as timestamp, and
        data entries for active power in W and state of charge in percent.
        This mutates the model on every call, independently of clock spacing.
        Errors are logged and return None; the logger must be initialized.
        """
        try:
            # update the asset status
            self.bss.update_bss_state(dt=self.update_interval)

            soc = self.bss.get_soc()*100.0
            power = self.bss.get_power()
            now = datetime.now(timezone.utc)
            dt = int(now.timestamp() * 1_000_000)

            return {
                # "asset_id": uuid.UUID(self.id).bytes,
                "asset_id": self.id,
                "timestamp": dt,
                "data": [
                    {"k": "active_power", "v": power, "u": "W"},
                    {"k": "state_of_charge", "v": soc, "u": "%"}
                ]
            }
        
        except Exception as e:
            self.logger.exception(f"Unexpected error in read_asset_status: {e}")

        return None

                
    async def set_power(self, power):
        """Apply a signed power request in watts through the BSS model's limits.

        Positive values discharge, negative values charge, and zero idles.
        Energy changes when the next status read advances the model.
        """
        self.bss.set_power(power)

    
    async def asset_status_loop(self):
        """Advance the model once and publish each snapshot channel as JSON.

        Despite its name, this method performs a single update without sleep
        or looping. A status read returning None or a publish error propagates
        to the caller.
        """
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
        """Log task failure and schedule one replacement status update.

        This callback is currently unused by run(). Cancellation is logged
        without restarting. A replacement receives no completion callback and
        performs only the single update implemented by asset_status_loop().
        """
        try:
            exception = task.exception()
            if exception:
                self.logger.error(f"Background task crashed: {exception}")
                self.asset_status_task = asyncio.create_task(self.asset_status_loop())
        except asyncio.CancelledError:
            self.logger.info("Background task was cancelled")


    async def process_queue(self):
        """Handle clocks and controls sequentially until cancelled or failed.

        Clock payloads require timestamp. When an update is due, last_update is
        set before awaiting a single status publication, regardless of how many
        intervals elapsed. Other subjects require k and v; active_power sets
        model power and other keys are ignored. Parsing and update errors
        propagate to run().
        """
        while True:
            msg = await self.queue.get()
            subject = msg.subject
            data = json.loads(msg.data.decode("utf-8"))
            self.logger.debug(f"{subject}: {data}")

            # clock: {'timestamp': '2026-03-12T12:40:00'}
            if 'clock' == subject:
                # update the local clocks
                clk = data['timestamp']
                self.update_clock(clk)
                # check if asset telemetry update is due
                if (self.clock - self.last_update) >= timedelta(seconds=self.update_interval):
                    self.last_update = self.clock 
                    # update the asset status
                    await self.asset_status_loop()

            else:
                key = data['k']
                value = data['v']
                
                if "active_power" == key:
                    await self.set_power(value)

    
    async def msg_handler(self, msg):
        """Queue a NATS clock or control message for ordered processing."""
        await self.queue.put(msg)


    async def run(self):
        """Start the asset event loop.

        This coroutine initializes the NATS message bus client, configures the
        logger, subscribes to the asset control topic and the shared ``clock``
        topic, and then blocks in :meth:`process_queue` to handle incoming
        commands and clock ticks for the lifetime of the process.

        Telemetry is awaited directly during clock processing; no periodic
        background task or task completion callback is enabled. Errors from
        queue processing propagate; no explicit NATS drain is provided here.
        """
        self.mbus = NATS()
        await self.mbus.connect("nats://natsio:4222")

        # setup logging over NATS
        # nats_logger.init(self.mbus)
        # self.logger = nats_logger.get_logger(self.name)

        self.logger = logging.getLogger(self.name)

        # subscribe to all messages
        await self.mbus.subscribe(f"{self.id}.control.*", cb=self.msg_handler)
        await self.mbus.subscribe(f"clock", cb=self.msg_handler)

        # await self.mbus.subscribe("*.control", cb=self.msg_handler)
        
        self.logger.debug(f"subscribed to: {self.id}.control")

        # start loop for periodic asset telemetry retrieval 
        # self.asset_status_task = asyncio.create_task(self.asset_status_loop())
        # self.asset_status_task.add_done_callback(self.task_done_callback)

        await self.process_queue()

        # while True:
        #     await asyncio.sleep(1)

    

if '__main__' == __name__:
    asset_id = os.getenv("ASSET_ID", None)
    asset_name = os.getenv("ASSET_NAME", None)
    asset_interface_config = json.loads(os.getenv("IF_CONFIG", None))
    asset_config = json.loads(os.getenv("DEV_CONFIG", None))
    topics = json.loads(os.getenv("TOPICS", None))
    root_topic = os.getenv("ROOT_TOPIC", None)
    polling_interval = os.getenv("POLLING_INTERVAL", None)

    print(asset_id)
    print(asset_name)
    print(asset_interface_config)
    print(asset_config)
    print(topics)
    print(polling_interval)

    asset = Asset(id=asset_id, name=asset_name, if_config=asset_interface_config, dev_config=asset_config, topics=topics, root_topic=root_topic, polling_interval=polling_interval)
    asyncio.run(asset.run())


