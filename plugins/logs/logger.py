"""Forward selected JSON message-bus traffic to a NATS debug-log subject.

Connect to ``nats://natsio:4222`` and subscribe to ``*.telemetry.>``,
``*.control.>``, ``*.status.>``, and the exact subject ``clock``. The first
wildcard matches one subject token (typically an asset/plugin ID); the trailing
wildcard requires at least one further token. For example,
``battery.telemetry.active_power`` matches, but ``battery.telemetry`` does not.

Messages are queued and processed in arrival order. Each payload must be UTF-8
JSON; MessagePack payloads are not decoded. The subject and decoded value are
formatted into a debug message and passed to ``utils.nats_logger``, which
publishes a JSON log entry on ``logs.nats_logger``. Entries contain timestamp,
level, process, and message fields. This plugin does not write a local log file.
Its subscriptions exclude log subjects, avoiding recursive logging of its own
output.

No environment configuration is read. Start this module as a script to run
indefinitely. Invalid UTF-8 or JSON stops queue processing; connection and
processing errors propagate without a retry loop. The queue is unbounded and
the log helper schedules publication without awaiting completion here.
"""

import os
import asyncio
import msgpack
from nats.aio.client import Client as NATS
import uuid
import json

import utils.nats_logger as nats_logger


class MainMessageBusLogger:
    """Receive selected NATS traffic and forward decoded payloads as debug logs.

    Construction creates the queue only. run() initializes the NATS connection
    and logger before subscribing and consuming messages.
    """

    def __init__(self):
        """Initialize an unbounded message queue and unset client/logger fields."""
        self._mbus = None
        self.logger = None
        self.queue = asyncio.Queue()


    async def _on_message(self, msg):
        """Queue an incoming NATS message for processing outside the callback."""
        await self.queue.put(msg)
    

    async def process_queue(self):
        """Decode queued JSON and log each subject and value at DEBUG level.

        Requires an initialized logger. Message payloads use the default UTF-8
        decoder; no schema validation is performed after JSON parsing. Decode
        and logging errors propagate, stopping this consumer.
        """
        while True:
            msg = await self.queue.get()
            data = json.loads(msg.data.decode())
            self.logger.debug(f"{msg.subject}: {data}")
            # self.logger.debug(f"{msg.subject}: {msgpack.unpackb(msg.data, raw=False)}")


    async def run(self):
        """Connect to NATS, configure the logger, subscribe, and consume traffic.

        Uses the fixed logger name nats_logger, publishing logs.nats_logger.
        Normally waits indefinitely in process_queue(). No explicit NATS drain
        is implemented. The trailing _client.loop_stop() refers to an undefined
        attribute and would fail if queue processing returned normally.
        """
        # start the NATS interface
        self._mbus = NATS()
        await self._mbus.connect("nats://natsio:4222")

        # setup logging over NATS
        nats_logger.init(self._mbus)
        self.logger = nats_logger.get_logger("nats_logger")

        # subscribe to all traffic
        await self._mbus.subscribe(f"*.telemetry.>", cb=self._on_message)
        await self._mbus.subscribe(f"*.control.>", cb=self._on_message)
        await self._mbus.subscribe(f"*.status.>", cb=self._on_message)
        await self._mbus.subscribe(f"clock", cb=self._on_message)

        # process all messages in a message queue from internal NATS-based message bus
        await self.process_queue()
        
        # will not happen due to process_queue infinite loop
        self._client.loop_stop()


if '__main__' == __name__:


    print("Starting NATS traffic logger")

    tlogger = MainMessageBusLogger()

    asyncio.run(tlogger.run())


        
        
