"""Bridge JSON NATS telemetry and MessagePack MQTT controls for a USG.

Outbound telemetry subscribes to ``*.telemetry.*`` on ``nats://natsio:4222``.
Each subject must have exactly three tokens, such as
``<asset-id>.telemetry.active_power``. Payloads must be UTF-8 JSON objects
containing ``aid`` as a UUID string. The bridge replaces aid with its 16 UUID
bytes, preserves other fields, encodes MessagePack, and publishes to the fixed
MQTT topic ``enpower/telemetry`` with QoS 0 and retain=False. ROOT_TOPIC does
not change this outbound topic. Aggregate two-token telemetry subjects and
MessagePack NATS payloads are not supported by this path.

Inbound controls subscribe to MQTT topic ``<ROOT_TOPIC>/<USG_ID>/control``.
MessagePack payloads must contain ``aid`` (16 UUID bytes), ``k`` (control key),
``v`` (value), and ``u`` (unit). The bridge converts aid to a UUID string and
publishes only these four fields as UTF-8 JSON to the exact NATS subject
``<asset-id>.control``. Drivers subscribing only to ``<asset-id>.control.*``
will not receive that two-token subject. The bridge performs no control-key,
unit, or value conversion beyond the UUID representation.

When launched as a script, the process manager supplies:

* ``MQTT_HOST``: broker hostname or address.
* ``MQTT_PORT``: integer-compatible broker port.
* ``MQTT_CERTS``: certificate directory ending in a path separator. The code
  concatenates ca-chain.cert.pem, client.cert.pem, and client.key.pem to it.
* ``USG_ID``: gateway identifier used in the MQTT control subscription.
* ``ROOT_TOPIC``: MQTT control topic prefix.

TLS uses the configured CA, client certificate, and private key. No MQTT
username/password authentication is configured. Paho uses callback API version
2 and its own network thread; inbound controls schedule NATS publications on
the asyncio event loop with run_coroutine_threadsafe(). MQTT starts before NATS
initialization completes, so an early control can arrive before NATS is ready.

Invalid configuration, certificates, telemetry JSON, or UUIDs propagate to the
caller. Control decoding errors are not handled within the MQTT callback;
asynchronous NATS publication failures are logged. MQTT publish results are
not checked, and no explicit connection drain or guaranteed delivery is provided.
"""

import os
import asyncio
import msgpack
from nats.aio.client import Client as NATS
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
import logging
import uuid
import json

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")


class MQTTProxy:
    """Manage the MQTT network thread and asyncio-based NATS telemetry bridge.

    Construction stores settings and creates an unbounded telemetry queue.
    run() initializes both clients and begins message processing.
    """

    def __init__(self, host, port, cert_path, usg_id, root_topic):
        """Store broker settings, certificate paths, and control topic identity.

        Convert port to an integer and concatenate certificate filenames onto
        cert_path, which must include the trailing directory separator.
        No broker connection or certificate validation occurs here.
        """
        self.name = "mqtt_proxy"
        self.hostname = host
        self.port = int(port)
        self._ca = cert_path + 'ca-chain.cert.pem'
        self._client_key = cert_path + 'client.key.pem'
        self._client_cert = cert_path + 'client.cert.pem'
        self._usg_id = usg_id
        self._root_topic = root_topic
        self._client = None
        self._mbus = None
        self.logger = logging.getLogger(self.name)
        self.queue = asyncio.Queue()
        self.loop = None


    def _on_mqtt_connect(self, client, userdata, flags, reason_code, properties):
        """Handle Paho's version-2 connection callback and subscribe to controls.

        Subscribe to <root_topic>/<usg_id>/control on each connection callback.
        The reason code is logged but not checked before subscribing; the
        subscription uses Paho's default QoS and its result is not checked.
        """
        self.logger.debug("Connected with result code %s", reason_code)
        # subscribe to all related topics based on USG ID
        topic = f"{self._root_topic}/{self._usg_id}/control"
        client.subscribe(topic)
        self.logger.debug("Subscribed to topic: %s", topic)


    def _on_mqtt_message(self, client, userdata, msg):
        """Decode a MessagePack control and schedule its JSON publication to NATS.

        Runs in Paho's network thread. Require aid as UUID bytes plus k, v,
        and u; discard other fields. Publish to the exact <asset-id>.control
        subject through the stored asyncio loop and log future publish errors.
        Decoding, missing-field, and scheduling errors propagate from this
        callback. NATS and the event loop must already be initialized.
        """
        # self.logger.debug(f"{msg.topic} -> {msg.payload.decode()}")
        # self.logger.debug(f"{msg.topic}: {msgpack.unpackb(msg.payload, raw=False)}")
        self.logger.debug(f"{msg.topic}: {msgpack.unpackb(msg.payload, raw=False)}")
        data = msgpack.unpackb(msg.payload, raw=False)
        asset_id = str(uuid.UUID(bytes=data['aid']))
        
        payload = {
            'aid': asset_id,
            'k': data['k'],
            'v': data['v'],
            'u': data['u']
        }

        self.logger.debug(f"sending control message to: {asset_id}.control; {payload}")

        # send to the control channel
        fut = asyncio.run_coroutine_threadsafe(
            self._mbus.publish(f"{asset_id}.control", json.dumps(payload).encode("utf-8")),
            self.loop
        )
        fut.add_done_callback(
            lambda f: self.logger.error(f"NATS publish error: {f.exception()}") if f.exception() else None
        )
        
    

    async def _on_mbus_message(self, msg):
        """Log receipt and enqueue a NATS message; currently not subscribed.

        Any caller using this helper must supply the same JSON telemetry
        format expected by process_queue().
        """
        self.logger.debug("mbus message")
        await self.queue.put(msg)


    async def _on_telemetry_message(self, msg):
        """Queue NATS telemetry for ordered conversion outside the callback."""
        await self.queue.put(msg)
    

    async def process_queue(self):
        """Convert queued JSON telemetry to MessagePack and publish it to MQTT.

        Replace aid's UUID string with bytes and preserve all other fields.
        Publish on enpower/telemetry without checking the returned MQTT result.
        JSON, UUID, and serialization errors propagate and stop this consumer.
        """
        while True:
            msg = await self.queue.get()
            # self.logger.debug(f"{msg.subject}: {msgpack.unpackb(msg.data, raw=False)}")
            data = json.loads(msg.data.decode())
            self.logger.debug(f"{msg.subject}: {data}")
            # transform asset_id into binary format
            data['aid'] = uuid.UUID(data['aid']).bytes
            topic = f"enpower/telemetry"
            data = msgpack.packb(data, use_bin_type=True)
            self._client.publish(topic=topic, payload=data, qos=0, retain=False)


    async def run(self):
        """Start TLS MQTT, connect NATS, subscribe to telemetry, and consume it.

        Store the running asyncio loop before starting Paho's network thread.
        MQTT callbacks may execute before NATS is connected. The initial
        telemetry subscription is *.telemetry.* and queue processing normally
        runs indefinitely. loop_stop() is reached only on a normal return;
        cancellation or exceptions do not explicitly close either client.
        """
        self.loop = asyncio.get_running_loop()
        # Set this before connecting: Paho invokes connection callbacks from
        # its network thread, which can run before the NATS setup completes.
        self.logger = logging.getLogger(self.name)

        self._client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2)

        # Assign callbacks
        self._client.on_connect = self._on_mqtt_connect
        self._client.on_message = self._on_mqtt_message

        self._client.tls_set(
            ca_certs=self._ca,
            certfile=self._client_cert,
            keyfile=self._client_key
        )

        # Connect to the broker (change host/port as needed)
        self._client.connect(self.hostname, port=self.port)
        self._client.loop_start()

        # start the NATS interface
        self._mbus = NATS()
        await self._mbus.connect("nats://natsio:4222")

        # subscribe to telemetry messages
        await self._mbus.subscribe(f"*.telemetry.*", cb=self._on_telemetry_message)

        # process all messages in a message queue from internal NATS-based message bus
        await self.process_queue()
        
        # will not happen due to process_queue infinite loop
        self._client.loop_stop()


if '__main__' == __name__:
    mqtt_host = os.getenv("MQTT_HOST", None)
    mqtt_port = os.getenv("MQTT_PORT", None)
    mqtt_certs = os.getenv("MQTT_CERTS", None)
    usg_id = os.getenv("USG_ID", None)
    root_topic = os.getenv("ROOT_TOPIC", None)

    print("Starting ECMP interface: MQTT")

    mqtt_proxy = MQTTProxy(host=mqtt_host, port=mqtt_port, cert_path=mqtt_certs, usg_id=usg_id, root_topic=root_topic)

    asyncio.run(mqtt_proxy.run())


        
        
