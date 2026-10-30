"""Expose a simulated battery storage system over NATS.

The driver wraps ``bss.BSS`` and advances the model once per polling interval,
using that interval in seconds as the simulation timestep. It uses local
asyncio timers, not the shared ``clock`` subject. The first update occurs after
one interval; delays in execution do not increase the model timestep.

Telemetry is UTF-8 JSON on ``<ASSET_ID>.telemetry.active_power`` and
``<ASSET_ID>.telemetry.state_of_charge``. Each message contains ``aid`` (asset
ID), ``ts`` (current UTC Unix time in microseconds), ``k`` (channel), ``v``
(numeric value), and ``u`` (``W`` or ``%``). State of charge is reported on a
0-100 scale. Positive active power means discharge; negative means charge.

Controls arrive on ``<ASSET_ID>.control.*`` as JSON with ``k`` and ``v`` fields.
For example, ``{"k": "active_power", "v": -1000.0}`` requests charging at
1000 W. The payload key selects the action regardless of the subject suffix;
keys other than ``active_power`` are ignored. Power and energy constraints
are applied by the BSS model. NATS connects to ``nats://natsio:4222``.

When launched as a script, the process manager supplies:

* ``ASSET_ID`` and ``ASSET_NAME``: telemetry subject prefix and logger name.
* ``IF_CONFIG``: JSON object with ``url``, ``user``, and ``password`` keys.
  These fields are required by construction but no device connection is made.
* ``DEV_CONFIG``: JSON object containing ``soc`` (initial fraction, 0-1),
  ``capacity`` (Wh), ``max_charge_power`` and ``max_discharge_power`` (W),
  and ``charge_efficiency`` and ``discharge_efficiency`` (fractions).
* ``POLLING_INTERVAL``: integer-compatible timestep and reporting period in
  seconds; configure a positive value.
* ``TOPICS``: valid JSON, stored for compatibility but not used for routing.
* ``ROOT_TOPIC``: stored for compatibility but not used in NATS subjects.

Missing configuration keys and malformed environment JSON fail startup.
Malformed control messages propagate out of the queue processor. Status-read
errors are logged and return None, which the telemetry loop does not handle;
see ``task_done_callback`` for its limited restart behavior.
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

import logging

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")


class Asset:
    """Manage a BSS model, periodic telemetry, and queued power commands.

    Construction initializes the model with zero power. ``run()`` initializes
    NATS and logging before starting telemetry and processing controls.
    """

    def __init__(self, id, name, if_config, dev_config, topics, root_topic, polling_interval):
        """Build the model from interface and device configuration dictionaries.

        ``polling_interval`` is converted to integer seconds. Interface fields,
        ``topics``, and ``root_topic`` are retained for driver compatibility;
        the simulation does not use them to communicate with a physical device.
        Device values are passed directly to ``BSS`` without validation here.
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

        # init simulated/emulated BSS
        self.bss=BSS(
            soc=dev_config['soc'], 
            capacity=dev_config['capacity'], 
            max_charge_power=dev_config['max_charge_power'], 
            max_discharge_power=dev_config['max_discharge_power'], 
            charge_efficiency=dev_config['charge_efficiency'], 
            discharge_efficiency=dev_config['discharge_efficiency']
        )
    

    async def read_asset_status(self):
        """Advance the model one timestep and return a telemetry snapshot.

        The result contains ``asset_id``, ``timestamp`` (UTC Unix microseconds),
        and ``data`` (active power in W and state of charge in percent).
        This method mutates the model on every call. Errors are logged and
        return None; the logger must already be initialized by ``run()``.
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
        """Apply a power request in watts through the BSS model's limits.

        Positive values discharge, negative values charge, and zero idles.
        Energy changes when the next status read advances the model.
        """
        self.bss.set_power(power)

    
    async def asset_status_loop(self):
        """Sleep, advance the model, and publish one JSON message per channel.

        Both channel messages share the snapshot timestamp. Read failures that
        return None and NATS publish failures terminate this background task.
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
        """Log telemetry task failure and start a replacement after an error.

        Cancellation is logged without restarting. The replacement task does
        not receive this callback, so automatic restart covers only the
        original task's failure.
        """
        try:
            exception = task.exception()
            if exception:
                self.logger.error(f"Background task crashed: {exception}")
                self.asset_status_task = asyncio.create_task(self.asset_status_loop())
        except asyncio.CancelledError:
            self.logger.info("Background task was cancelled")


    async def process_queue(self):
        """Consume JSON controls in arrival order and apply active_power values.

        Every payload must contain ``k`` and ``v``. Other keys are ignored;
        decoding errors, missing fields, and model errors propagate to ``run``.
        """
        while True:
            msg = await self.queue.get()
            subject = msg.subject
            data = json.loads(msg.data.decode("utf-8"))
            self.logger.debug(f"{subject}: {data}")

            key = data['k']
            value = data['v']
            
            if "active_power" == key:
                await self.set_power(value)

    
    async def msg_handler(self, msg):
        """Enqueue a NATS control message for processing outside the callback."""
        await self.queue.put(msg)


    async def run(self):
        """Connect to NATS and run telemetry alongside control processing.

        Subscribes to ``<id>.control.*`` and attaches a completion callback to
        the initial telemetry task. Runs until cancelled or control processing
        fails; no explicit NATS drain or background-task cleanup is provided.
        """
        self.mbus = NATS()
        await self.mbus.connect("nats://natsio:4222")

        # setup logging over NATS
        # nats_logger.init(self.mbus)
        # self.logger = nats_logger.get_logger(self.name)
        self.logger = logging.getLogger(self.name)

        # subscribe to all messages
        await self.mbus.subscribe(f"{self.id}.control.*", cb=self.msg_handler)
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


