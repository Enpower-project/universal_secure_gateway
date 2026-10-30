import json
import asyncio
import datetime
from datetime import datetime, timezone

class NATSLogger:
    def __init__(self, name, nc, loop):
        self.name = name
        self.nc = nc
        self.loop = loop

    def _log(self, level, message):
        log_entry = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.")+ f"{int(datetime.now(timezone.utc).microsecond / 1000):03d}Z",
            "level": level,
            "process": self.name,
            "message": message
        }
        asyncio.run_coroutine_threadsafe(
            self.nc.publish(f"logs.{self.name}", json.dumps(log_entry).encode()),
            self.loop
        )

    def info(self, msg): self._log("INFO", msg)
    def debug(self, msg): self._log("DEBUG", msg)
    def warning(self, msg): self._log("WARNING", msg)
    def exception(self, msg): self._log("EXCEPTION", msg)
    def error(self, msg): self._log("ERROR", msg)
    def critical(self, msg): self._log("CRITICAL", msg)

# Global connection and loop
_nc = None
_loop = None


def init(nc, loop=None):
    # use NATS client from parent process for init
    global _nc, _loop
    _nc = nc
    _loop = loop or asyncio.get_event_loop()


def get_logger(name):
    if _nc is None or _loop is None:
        raise RuntimeError("nats_logger not initialized.")
    return NATSLogger(name, _nc, _loop)