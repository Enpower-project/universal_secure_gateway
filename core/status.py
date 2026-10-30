import os
import subprocess
import asyncio
from nats.aio.client import Client as NATS
from datetime import datetime, timezone
import yaml
import json

# import utils.nats_logger as nats_logger
import logging

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")


class StatusManager:
    def __init__(self):
        self.asset_processes = {}
        self.plugin_processes = {}
        self.ecmp_process = None
        self.mbus = None
        self.queue = asyncio.Queue()
    
        self.STATUS_STORE = {
            "auth": {
                "service": "auth",
                "overall_status": "healthy",
                "last_updated": self.utc_now(),
                "instances": [
                    {
                        "instance_id": "auth-1",
                        "status": "healthy",
                        "last_heartbeat": self.utc_now(),
                    }
                ],
            },
            "billing": {
                "service": "billing",
                "overall_status": "degraded",
                "last_updated": self.utc_now(),
                "instances": [
                    {
                        "instance_id": "billing-1",
                        "status": "degraded",
                        "last_heartbeat": self.utc_now(),
                    }
                ],
            },
        }

    def utc_now(self):
        return datetime.now(timezone.utc).isoformat()
    
    async def handle_getreq_msg(self, msg):
        try:
            payload = json.loads(msg.data.decode()) if msg.data else {}
        except json.JSONDecodeError:
            await msg.respond(json.dumps({"error": "invalid json"}).encode())
            return
        
        service = payload.get("service")

        if not service:
            response = {
                "services": {
                    name: {
                        "service": data["service"],
                        "overall_status": data["overall_status"],
                        "last_updated": data["last_updated"],
                    }
                    for name, data in self.STATUS_STORE.items()
                }
            }
            await msg.respond(json.dumps(response).encode())
            return
        
        data = self.STATUS_STORE.get(service)
        if not data:
            await msg.respond(json.dumps({"error": f"service '{service}' not found"}).encode())
            return

        response = dict(data)

        await msg.respond(json.dumps(response).encode())
    

    async def handle_telemetry_update(self, msg):
        pass


    async def handle_control_update(self, msg):
        pass
            
                
    async def run(self):
        self.mbus = NATS()
        await self.mbus.connect("nats://natsio:4222")

        # setup logging over NATS
        # nats_logger.init(self.mbus)
        # self.logger = nats_logger.get_logger("status_manager")
        self.logger = logging.getLogger("status_manager")

        await self.mbus.subscribe("status.get", cb=self.handle_getreq_msg)
        await self.mbus.subscribe("*.telemetry.>", cb=self.handle_telemetry_update)
        await self.mbus.subscribe("*.control.>", cb=self.handle_control_update)


        try:
            while True:
                await asyncio.sleep(3600)
        finally:
            # drain before exit so buffered work finishes cleanly
            await self.mbus.drain()



if __name__ == "__main__":
    manager = StatusManager()
    asyncio.run(manager.run())
    # try:
    #     asyncio.run(manager.run())
    #     # input("Press Enter to shut down...\n")
    # finally:
    #     manager.shutdown()



