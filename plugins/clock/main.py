"""Publish a simulation clock over NATS at a configurable real-time cadence.

After each ``tick`` seconds of asyncio sleep, advance simulation time by
``timestep`` seconds and publish UTF-8 JSON on the shared ``clock`` subject,
for example ``{"timestamp": "2026-09-29T22:15:00"}``. The starting timestamp
itself is not published. With tick=1 and timestep=900, each iteration advances
simulation time by 15 minutes. Processing and publication time add to the sleep;
there is no correction for drift or synchronization with wall-clock time.

When launched as a script, the process manager supplies ``PLUGIN_ID`` (control
subject prefix), ``PLUGIN_NAME`` (logger name), and ``PLUGIN_CONFIG`` (JSON).
The configuration must contain:

* ``tick``: numeric sleep duration in seconds; use a positive value.
* ``timestep``: numeric simulation-time increment in seconds.
* ``start_datetime``: initial time in ``YYYY-MM-DDTHH:MM:SS`` format.
* ``end_datetime``: time in the same format; required by the entry point but
  only stored, not used to stop publication.

For example, PLUGIN_CONFIG may be
``{"tick": 1, "timestep": 900, "start_datetime": "2026-09-29T22:00:00",
"end_datetime": "2026-09-30T22:00:00"}``. The entry point parses naive
datetimes; outgoing timestamps have neither timezone offsets nor fractions.
Direct callers supplying aware datetimes also lose the offset when formatting.

NATS connects to ``nats://natsio:4222``. Controls are JSON on
``<PLUGIN_ID>.control.*`` with ``k`` and ``v`` fields. The only recognized key
is ``clock_active`` with the case-sensitive string value ``"True"`` or
``"False"``, for example ``{"k": "clock_active", "v": "False"}``.
JSON booleans are not recognized. The payload key selects the action regardless
of the subject suffix.

Setting clock_active to False ends the update loop at its next condition check;
an iteration already sleeping can still publish one more tick. Setting it back
to True after the task has exited does not create a new task, so this is not a
complete pause/resume mechanism. Malformed controls propagate out of queue
processing. Missing or invalid environment configuration fails startup.
"""

import os
import asyncio
from nats.aio.client import Client as NATS
import aiohttp
import json
import msgpack
from datetime import datetime, timezone, timedelta

import logging

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")


class Clock:
    """Hold simulation time, publish clock ticks, and process activation controls.

    Construction starts with clock_active=True. Call run() to initialize NATS
    and logging, then start the publisher alongside queued control processing.
    """

    def __init__(self, id, name, tick, timestep, start_datetime, end_datetime):
        """Store plugin identity, timing values, and datetime boundaries.

        tick is the real-time sleep in seconds; timestep is the simulation
        increment in seconds. start_datetime and end_datetime are datetime
        objects. No type conversion or validation is performed here, and
        end_datetime is retained without enforcing a stopping condition.
        """
        self.id = id
        self.name = name
        self.tick = tick
        self.timestep = timestep
        self.time = start_datetime
        self.end_datetime = end_datetime
        self.clock_active = True
        self.update_task = None
        self.mbus = None
        self.logger = None
        self.queue = asyncio.Queue()

    
    async def clock_update_loop(self):
        """Sleep, advance simulation time, and publish until deactivated.

        Each message contains a timestamp formatted to whole seconds without
        a timezone offset. The active flag is checked before each sleep, not
        again before publication. Publish errors propagate after time has
        already advanced; end_datetime is not checked.
        """
        while True == self.clock_active:
            await asyncio.sleep(self.tick)
            # self.logger.debug("Update clock with timestep")
            self.time = self.time + timedelta(seconds=self.timestep)
            timestr = self.time.strftime("%Y-%m-%dT%H:%M:%S")

            # self.logger.debug(f"New datetime: {timestr}")
            data = {
                "timestamp": timestr
            }
            payload = json.dumps(data).encode()

            await self.mbus.publish("clock", payload)
    

    def task_done_callback(self, task):
        """Log publisher failure and start one replacement task after an error.

        Normal completion and cancellation do not restart publication. The
        replacement is stored as asset_status_task rather than update_task
        and receives no callback, so subsequent failures do not restart it.
        """
        try:
            exception = task.exception()
            if exception:
                self.logger.error(f"Background task crashed: {exception}")
                self.asset_status_task = asyncio.create_task(self.clock_update_loop())
        except asyncio.CancelledError:
            self.logger.info("Background task was cancelled")


    async def process_queue(self):
        """Apply queued clock_active controls in arrival order.

        Decode JSON with required k and v fields. Only the exact strings
        "True" and "False" change the flag; other keys and values are ignored.
        This method does not restart an exited publisher. Decode errors and
        missing fields propagate to run().
        """
        while True:
            msg = await self.queue.get()
            subject = msg.subject
            data = json.loads(msg.data.decode("utf-8"))
            self.logger.debug(f"{subject}: {data}")

            key = data['k']
            value = data['v']
            
            if "clock_active" == key:
                if "True" == value:
                    self.clock_active = True
                elif "False" == value:
                    self.clock_active = False

    
    async def msg_handler(self, msg):
        """Queue a NATS control message for processing outside the callback."""
        await self.queue.put(msg)


    async def run(self):
        """Connect to NATS and start clock publication and control processing.

        Subscribe to <id>.control.*, attach the completion callback to the
        initial update_task, and await the queue processor. No explicit NATS
        drain or publisher cancellation is implemented on exit.
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
        
        # self.logger.debug(f"subscribed to: {self.id}.control")

        # start the main task loop 
        self.update_task = asyncio.create_task(self.clock_update_loop())
        self.update_task.add_done_callback(self.task_done_callback)

        await self.process_queue()

        # while True:
        #     await asyncio.sleep(1)

    

if '__main__' == __name__:
    print("test")
    plugin_id = os.getenv("PLUGIN_ID", None)
    print(plugin_id)
    plugin_name = os.getenv("PLUGIN_NAME", None)
    print(plugin_name)
    PLUGIN_CONFIG = json.loads(os.getenv("PLUGIN_CONFIG", None))
    print(PLUGIN_CONFIG)
    tick = PLUGIN_CONFIG.get("tick")
    print(tick)
    timestep = PLUGIN_CONFIG.get("timestep")
    print(timestep)
    start_datetime = PLUGIN_CONFIG.get("start_datetime")
    start_datetime = datetime.strptime(start_datetime, "%Y-%m-%dT%H:%M:%S")
    print(start_datetime)
    end_datetime = PLUGIN_CONFIG.get("end_datetime")
    end_datetime = datetime.strptime(end_datetime, "%Y-%m-%dT%H:%M:%S")
    print(end_datetime)

    proc = Clock(id=plugin_id, name=plugin_name, tick=tick, timestep=timestep, start_datetime=start_datetime, end_datetime=end_datetime)
    asyncio.run(proc.run())
