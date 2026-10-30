"""Replay XLSX measurements through NATS using the simulated clock.

The driver loads ``xl/worksheets/sheet1.xml`` once at startup. Its first row
must contain a ``timestamp`` header followed by supported measurement headers
(see ``SMAsset.CHANNELS``). Timestamps must be numeric Excel serial dates using
the 1899-12-30 epoch; measurement values must be numeric watts. Headers are
case-insensitive, with bracketed units and punctuation removed before matching.
For example, ``Meter Import [W]`` maps to ``import_power``.

The lightweight XLSX reader supports inline strings and raw cell values; it
does not resolve shared strings, evaluate formulas, or use cell references to
align sparse rows. Use inline-string headers and explicitly populated cells
to preserve column alignment. Duplicate timestamps keep the last row.

Each JSON message on NATS subject ``clock`` must contain an ISO 8601
``timestamp``, for example ``{"timestamp": "2026-09-29T22:00:00Z"}``.
Only an exact timestamp match publishes measurements: there is no interpolation
or periodic polling. Repeated clock timestamps publish the same row again.
Clock offsets are converted to UTC and microseconds are discarded. Workbook
timestamps and clock timestamps without an offset are treated as UTC.

Each measurement is published on ``<ASSET_ID>.telemetry.<channel>`` as UTF-8
JSON: ``{"aid": "...", "ts": 1790719200000000, "k": "import_power",
"v": 1200.0, "u": "W"}``. ``ts`` is Unix time in microseconds.
The NATS URL is fixed at ``nats://natsio:4222``.

When run as a script, the process manager supplies these environment variables:

* ``ASSET_ID`` and ``ASSET_NAME``: telemetry subject prefix and logger name.
* ``IF_CONFIG``: JSON object with a ``file`` path accessible inside the container,
  for example ``{"file": "/code/data/measurements.xlsx"}``.
* ``POLLING_INTERVAL``: integer-compatible value retained for compatibility;
  publication is driven entirely by clock messages.
* ``DEV_CONFIG`` and ``TOPICS``: valid JSON, currently unused by the driver.
* ``ROOT_TOPIC``: accepted for compatibility; not used in NATS subjects.

Workbook, configuration, and malformed clock-message errors propagate to the
caller. A valid clock timestamp with no matching row simply publishes nothing.
"""

import asyncio
import json
import logging
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zipfile import ZipFile

from nats.aio.client import Client as NATS


logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")


class SMAsset:
    """Hold the workbook index and publish rows selected by NATS clock messages.

    Construction loads the workbook synchronously. Call ``run()`` to connect
    to NATS, initialize the logger, and consume messages in arrival order.
    """

    # Keep NATS keys stable while allowing human-readable spreadsheet headers.
    CHANNELS = {
        "meter_import": "import_power",
        "meter_export": "export_power",
        "pv_export_power": "pv_production",
        "bss_charge_power": "charge_power",
        "bss_discharge_power": "discharge_power",
    }
    EXCEL_EPOCH = datetime(1899, 12, 30)
    XLSX_NS = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

    def __init__(self, id, name, if_config, dev_config, topics, root_topic, polling_interval):
        """Initialize an asset using the process manager's configuration values.

        ``if_config['file']`` selects the workbook. Relative paths resolve from
        the process working directory. ``dev_config`` is accepted but unused;
        ``topics``, ``root_topic``, and ``polling_interval`` do not affect replay.
        Missing files, invalid workbook data, or invalid intervals fail startup.
        """
        self.id = id
        self.name = name
        self.file_path = Path(if_config["file"])
        self.topics = topics
        self.root_topic = root_topic
        self.update_interval = int(polling_interval)
        self.clock = None
        self.previous_clock = None
        self.measurements = self.load_measurements()
        self.mbus = None
        self.logger = None
        self.queue = asyncio.Queue()

    @staticmethod
    def _normalise_header(header):
        """Return a unit-free, snake-case XLSX column name."""
        without_unit = re.sub(r"\s*\[[^]]*\]", "", header.lower())
        return re.sub(r"[^a-z0-9]+", "_", without_unit).strip("_")

    @staticmethod
    def _cell_value(cell):
        """Extract inline text or the raw XML value, returning None if absent.

        Shared-string indices and cached formula values are returned as stored;
        this helper does not interpret cell styles or workbook metadata.
        """
        if cell.get("t") == "inlineStr":
            return "".join(cell.itertext())
        value = cell.find("x:v", SMAsset.XLSX_NS)
        return None if value is None else value.text

    @classmethod
    def _excel_datetime(cls, value):
        """Convert an Excel serial date to a naive datetime rounded to seconds."""
        # XLSX dates are days since 1899-12-30. Rounding removes float noise.
        return cls.EXCEL_EPOCH + timedelta(seconds=round(float(value) * 86_400))

    def load_measurements(self):
        """Return a datetime-to-row index from the first worksheet XML.

        Each row contains ``timestamp`` and a ``data`` list of ``k``, ``v``,
        and ``u`` measurement dictionaries. Unsupported headers are ignored;
        absent measurement values are omitted. Rows without timestamps are
        skipped and later duplicate timestamps replace earlier ones.

        Raises FileNotFoundError for a missing file and ValueError for missing
        worksheet/header/channel data or invalid numeric values. ZIP and XML
        parsing errors also propagate.
        """
        if not self.file_path.is_file():
            raise FileNotFoundError(f"Configured measurement file does not exist: {self.file_path}")

        with ZipFile(self.file_path) as workbook:
            try:
                sheet = ET.fromstring(workbook.read("xl/worksheets/sheet1.xml"))
            except KeyError as exc:
                raise ValueError(f"No first worksheet found in {self.file_path}") from exc

        rows = sheet.findall(".//x:sheetData/x:row", self.XLSX_NS)
        if not rows:
            raise ValueError(f"Measurement workbook is empty: {self.file_path}")
        headers = [self._cell_value(cell) for cell in rows[0].findall("x:c", self.XLSX_NS)]
        if not headers or not headers[0] or self._normalise_header(headers[0]) != "timestamp":
            raise ValueError(f"First column in {self.file_path} must be named 'timestamp'")

        channel_columns = {
            index: self.CHANNELS[normalised]
            for index, header in enumerate(headers[1:], start=1)
            if header is not None and (normalised := self._normalise_header(header)) in self.CHANNELS
        }
        if not channel_columns:
            raise ValueError(f"No supported measurement columns found in {self.file_path}")

        measurements = {}
        for row in rows[1:]:
            values = [self._cell_value(cell) for cell in row.findall("x:c", self.XLSX_NS)]
            if not values or values[0] is None:
                continue
            row_timestamp = self._excel_datetime(values[0])
            measurements[row_timestamp] = {
                "timestamp": row_timestamp,
                "data": [
                {"k": key, "v": float(values[index]), "u": "W"}
                for index, key in channel_columns.items()
                if index < len(values) and values[index] is not None
                ],
            }
        if not measurements:
            raise ValueError(f"No timestamped measurements found in {self.file_path}")
        return measurements

    @staticmethod
    def _parse_clock(clock):
        """Parse ISO 8601 text into a naive UTC datetime at second precision."""
        dt = datetime.fromisoformat(clock.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.replace(microsecond=0)

    def update_clock(self, clock):
        """Store the previous clock and parse the incoming timestamp string.

        The previous value is retained for diagnostics; it does not restrict
        replay direction or suppress repeated timestamps.
        """
        self.previous_clock = self.clock
        self.clock = self._parse_clock(clock)
        self.logger.debug("clock: %s, previous_clock: %s", self.clock, self.previous_clock)

    async def read_asset_status(self):
        """Return the matching row as asset_id, Unix microseconds, and data.

        Returns None when no row matches. Requires an initialized logger and
        a clock set by ``update_clock()``.
        """
        row = self.measurements.get(self.clock)
        if row is None:
            self.logger.debug("No measurements for clock timestamp %s", self.clock.isoformat())
            return None
        timestamp = int(row["timestamp"].replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
        return {"asset_id": self.id, "timestamp": timestamp, "data": row["data"]}

    async def asset_status_loop(self):
        """Publish each channel for the current clock once, if a row exists.

        Despite its name, this method handles a single update, not a loop.
        """
        status = await self.read_asset_status()
        self.logger.debug(f"status: {status}")
        if status is None:
            return
        for ch_report in status["data"]:
            data = {"aid": status["asset_id"], "ts": status["timestamp"], **ch_report}
            await self.mbus.publish(f"{self.id}.telemetry.{data['k']}", json.dumps(data).encode())

    async def process_queue(self):
        """Consume clock messages serially and publish each matching row.

        Other subjects, including subscribed control messages, are ignored.
        Invalid JSON, missing timestamps, and parsing or publish errors
        propagate and stop queue processing.
        """
        while True:
            msg = await self.queue.get()
            if msg.subject != "clock":
                continue
            data = json.loads(msg.data.decode("utf-8"))
            self.update_clock(data["timestamp"])
            await self.asset_status_loop()

    async def msg_handler(self, msg):
        """Queue a NATS message for ordered processing outside the callback."""
        await self.queue.put(msg)

    async def run(self):
        """Connect to NATS, subscribe to clock and control subjects, and replay.

        Runs until cancelled or an error propagates. Control messages are
        subscribed for interface compatibility but have no effect on replay.
        """
        self.mbus = NATS()
        await self.mbus.connect("nats://natsio:4222")
        self.logger = logging.getLogger(self.name)
        self.logger.info("Loaded %d timestamped measurements from %s", len(self.measurements), self.file_path)
        await self.mbus.subscribe(f"{self.id}.control.*", cb=self.msg_handler)
        await self.mbus.subscribe("clock", cb=self.msg_handler)
        await self.process_queue()


if __name__ == "__main__":
    asset = SMAsset(
        id=os.getenv("ASSET_ID"),
        name=os.getenv("ASSET_NAME"),
        if_config=json.loads(os.getenv("IF_CONFIG")),
        dev_config=json.loads(os.getenv("DEV_CONFIG")),
        topics=json.loads(os.getenv("TOPICS")),
        root_topic=os.getenv("ROOT_TOPIC"),
        polling_interval=os.getenv("POLLING_INTERVAL"),
    )
    asyncio.run(asset.run())
