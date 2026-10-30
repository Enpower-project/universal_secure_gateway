"""Poll a Sonnen Performance 10 status API and publish snapshots over NATS.

The driver GETs ``<IF_CONFIG.url>/api/v2/status`` with a ten-second request
timeout. Each polling iteration first sleeps one second, so its total duration
also includes HTTP and publication time. Each request uses a new HTTP session.
Status requests send no authentication credentials or token headers.

Telemetry is one MessagePack snapshot on ``<ASSET_ID>.telemetry`` containing
``asset_id`` (16 UUID bytes), ``timestamp`` (current UTC Unix microseconds),
and ``data`` (a list of dictionaries with ``k``, ``v``, and ``u`` fields).
The API-to-channel mapping is:

* BatteryCharging -> charging (bool); BatteryDischarging -> discharging (bool).
* Consumption_Avg -> consumption_avg (W); Consumption_W -> consumption (W).
* Fac -> frequency (Hz); GridFeedIn_W -> grid_feed_in (W).
* OperatingMode -> mode (empty unit); Pac_total_W -> inverter_power (W).
* Production_W -> production (W); RSOC -> soc (%).
* RemainingCapacity_Wh -> stored_energy (Wh); USOC -> user_soc (%).
* Uac -> grid_voltage (V); Ubat -> battery_voltage (V).

Values are copied directly without scaling or sign conversion. Missing fields
remain None. Failed status reads return None, which is published as MessagePack
nil rather than skipped.

NATS connects to ``nats://natsio:4222``. Logs are JSON messages on
``logs.<ASSET_NAME>`` through ``utils.nats_logger``. No NATS subscriptions are
enabled in run(); queued messages are only logged. The discharge helper is not
connected to NATS controls and currently constructs a set for HTTP params;
its implementation does not establish a working setpoint request format.

When launched as a script, the process manager supplies:

* ``ASSET_ID``: UUID string converted to bytes in telemetry.
* ``ASSET_NAME``: logger name.
* ``IF_CONFIG``: JSON object with ``url``, ``user``, and ``password`` keys.
  Use a base URL without a trailing slash. Credentials are used only in the
  discharge helper's JSON body.
* ``TOPICS``: valid JSON, stored but not used for NATS routing.
* ``ROOT_TOPIC``: stored but not used in NATS subjects.

Missing configuration keys and invalid environment JSON fail startup. HTTP,
response-decoding, and snapshot-construction failures are logged by the status
reader. Unhandled telemetry-task failures have limited restart behavior;
see task_done_callback().
"""

import os
import asyncio
from nats.aio.client import Client as NATS
import aiohttp
import json
import msgpack
import uuid
from datetime import datetime, timezone

import utils.nats_logger as nats_logger


class Asset:
    """Manage HTTP status polling, NATS telemetry, and a message logging queue.

    Construction stores configuration. Call run() to initialize NATS and the
    logger, start telemetry, and wait for queued messages.
    """

    def __init__(self, id, name, if_config, topics, root_topic):
        """Store identity and required url, user, and password interface keys.

        topics and root_topic are retained for compatibility but do not select
        publication subjects. No device or NATS connection is created here.
        """
        self.id = id
        self.name = name
        self.if_url = if_config['url']
        self.if_usr = if_config['user']
        self.if_pwd = if_config['password']
        self.topics = topics
        self.root_topic = root_topic
        self.mbus = None
        self.logger = None
        self.queue = asyncio.Queue()
    

    async def read_asset_status(self):
        """GET /api/v2/status and map its JSON fields into a telemetry snapshot.

        Require HTTP 200 and a decodable JSON response. Return asset_id as UUID
        bytes, timestamp as UTC Unix microseconds, and the channel data listed
        in the module documentation. Missing API fields produce None values.
        Handled request, JSON, and mapping errors are logged and return None.
        The logger must already be initialized by run().
        """
        headers = {
            'Content-Type': 'application/json; charset=utf-8'
        }

        url = self.if_url + '/api/v2/status'

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url=url, headers=headers, timeout=10) as resp:
                    if resp.status != 200:
                        self.logger.error(f"Failed to fetch status from {url}: HTTP {resp.status}")
                        return None

                    try:
                        content = await resp.json()
                    except aiohttp.ContentTypeError:
                        self.logger.exception("Invalid content-type in response (not JSON)")
                        return None
                    except Exception as e:
                        self.logger.exception(f"Error parsing JSON: {e}")
                        return None

                    now = datetime.now(timezone.utc)
                    dt = int(now.timestamp() * 1_000_000)

                    return {
                        "asset_id": uuid.UUID(self.id).bytes,
                        "timestamp": dt,
                        "data": [
                            {"k": "charging", "v": content.get('BatteryCharging'), "u": "bool"},
                            {"k": "discharging", "v": content.get('BatteryDischarging'), "u": "bool"},
                            {"k": "consumption_avg", "v": content.get('Consumption_Avg'), "u": "W"},
                            {"k": "consumption", "v": content.get('Consumption_W'), "u": "W"},
                            {"k": "frequency", "v": content.get('Fac'), "u": "Hz"},
                            {"k": "grid_feed_in", "v": content.get('GridFeedIn_W'), "u": "W"},
                            {"k": "mode", "v": content.get('OperatingMode'), "u": ""},
                            {"k": "inverter_power", "v": content.get('Pac_total_W'), "u": "W"},
                            {"k": "production", "v": content.get('Production_W'), "u": "W"},
                            {"k": "soc", "v": content.get('RSOC'), "u": "%"},
                            {"k": "stored_energy", "v": content.get('RemainingCapacity_Wh'), "u": "Wh"},
                            {"k": "user_soc", "v": content.get('USOC'), "u": "%"},
                            {"k": "grid_voltage", "v": content.get('Uac'), "u": "V"},
                            {"k": "battery_voltage", "v": content.get('Ubat'), "u": "V"}
                        ]
                    }

        except aiohttp.ClientConnectorError as e:
            self.logger.exception(f"Connection error while trying to reach {url}: {e}")
        except aiohttp.ClientError as e:
            self.logger.exception(f"Client error while requesting {url}: {e}")
        except Exception as e:
            self.logger.exception(f"Unexpected error in read_asset_status: {e}")

        return None

                
    async def set_discharge_power(self, discharge_power):
        """Attempt a POST to /api/v2/setpoint/discharge and log its JSON reply.

        Send user/password in the JSON body. The current params argument is a
        Python set containing str(discharge_power), rather than a query mapping
        or sequence of key/value pairs, and can fail request construction.
        No input validation, HTTP-status check, or explicit timeout is supplied.
        Request and response-decoding errors propagate. This helper is not
        called by queue processing and returns no success value.
        """
        headers = {
            'Content-Type': 'application/json; charset=utf-8'
        }
        data = {
            'user': self.if_usr,
            'password': self.if_pwd
        }
        params = {
            str(discharge_power)
            }

        url = self.if_url + '/api/v2/setpoint/discharge'


        async with aiohttp.ClientSession() as session:
            async with session.post(url=url, json=data, headers=headers, params=params) as resp:
                content = await resp.json()
                self.logger.debug(content)

    
    async def asset_status_loop(self):
        """Sleep one second, read status, and publish a MessagePack snapshot.

        Publish None as MessagePack nil when a read fails. Serialization and
        NATS publish errors propagate and terminate this background task.
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
        """Log queued message subjects and UTF-8 payloads without dispatching controls.

        No subscriptions are enabled in run(), so this waits unless a caller
        explicitly enqueues messages. Payload decoding errors propagate.
        """
        while True:
            msg = await self.queue.get()
            self.logger.debug(f"{msg.subject}: {msg.data.decode()}")

    
    async def msg_handler(self, msg):
        """Enqueue a NATS message for logging outside the callback."""
        await self.queue.put(msg)


    async def run(self):
        """Connect to NATS, configure NATS logging, and start status polling.

        Attach the completion callback to the initial telemetry task, then
        await queue processing. No NATS subscriptions, explicit connection
        drain, or background-task cancellation are implemented here.
        """
        self.mbus = NATS()
        await self.mbus.connect("nats://natsio:4222")

        # setup logging over NATS
        nats_logger.init(self.mbus)
        self.logger = nats_logger.get_logger(self.name)

        # subscribe to all messages
        # await self.mbus.subscribe(f">", cb=self.msg_handler)

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


