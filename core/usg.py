import os
import subprocess
import asyncio
from nats.aio.client import Client as NATS
import yaml
import logging
import json


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class ProcessManager:
    def __init__(self, config_file):
        self.config_file = config_file
        self.config = self.load_config()
        self.status_process = None
        self.asset_processes = {}
        self.plugin_processes = {}
        self.ecmp_process = None
        self.mbus = None
        self.queue = asyncio.Queue()


    def load_config(self):
        with open(self.config_file, 'r') as f:
            return yaml.safe_load(f)
        
        
    def start_processes(self):
        logger.debug(f"status config: {self.config['status']}")
        # start plugins
        if None != self.config['plugins']:
            for plugin in self.config['plugins']:
                self.start_plugin_process(plugin)
        # start status manager process
        if None != self.config['status']:
            self.start_status_process()
        # start ecmp process
        logger.debug(f"ecmp config: {self.config['ecmp']}")
        # start ECMP connection interface if configuration exists
        if None != self.config['ecmp']:
            self.start_ecmp_process()
        # start asset processes
        for asset in self.config['assets']:
            self.start_asset_process(asset)
        
    

    def start_status_process(self):
        cmd = self.config['status']['exec'].split()
        logger.debug(cmd)
        proc = subprocess.Popen(cmd)
        self.status_process = proc
        logger.info(f"Started status manager process with PID {proc.pid}")


    def start_ecmp_process(self):
        cmd = self.config['ecmp']['exec'].split()
        logger.debug(cmd)
        env = os.environ.copy()
        env["MQTT_HOST"] = self.config['ecmp']["host"]
        env["MQTT_PORT"] = str(self.config['ecmp']["port"])
        env["MQTT_CERTS"] = self.config['ecmp']['cert']
        env["USG_ID"] = self.config['usg_id']
        env["ROOT_TOPIC"] = self.config['root_topic']
        proc = subprocess.Popen(cmd, env=env)
        self.ecmp_process = proc
        logger.info(f"Started ECMP process with PID {proc.pid}")


    def start_plugin_process(self, plugin):
        cmd = plugin['exec'].split()
        env = os.environ.copy()
        env["PLUGIN_ID"] = plugin["id"]
        env["PLUGIN_NAME"] = plugin["name"]
        env["PLUGIN_CONFIG"] = json.dumps(plugin["config"])
        logger.info(f"Plugin config: {plugin["config"]}")
        proc = subprocess.Popen(cmd, env=env)
        self.plugin_processes[plugin["id"]] = proc
        logger.info(f"Started {plugin['id']} with PID {proc.pid}")
    

    def start_asset_process(self, asset):
        logger.debug(f"asset: {asset}")
        cmd = asset['exec'].split()
        env = os.environ.copy()
        env["ASSET_ID"] = asset["id"]
        env["ASSET_NAME"] = asset["name"]
        env["IF_CONFIG"] = json.dumps(asset['device_interface']['config'])
        env["DEV_CONFIG"] = json.dumps(asset['device_config'])
        env["TOPICS"] = json.dumps(asset['topics'])
        env["ROOT_TOPIC"] = self.config['root_topic']
        env["POLLING_INTERVAL"] = json.dumps(asset['polling_interval'])
        proc = subprocess.Popen(cmd, env=env)
        self.asset_processes[asset["id"]] = proc
        logger.info(f"Started {asset['id']} with PID {proc.pid}")


    def stop_asset_process(self, asset_id):
        proc = self.asset_processes.pop(asset_id, None)
        if proc:
            proc.terminate()

    
    async def logs_msg_handler(self, msg):
        await self.queue.put(("logs", msg))


    async def process_queue(self):
        while True:
            source, msg = await self.queue.get()
            logger.debug(f"[{source}] {msg.subject}: {msg.data.decode()}")
    

    async def process_status_check_loop(self):
        while True:
            await asyncio.sleep(1)
            try:
                # logger.debug("Check assets' status")
                # check ECMP process if active
                if None != self.ecmp_process:
                    logger.debug(self.ecmp_process.poll())
                # for process in self.asset_processes:
                #     logger.debug(self.asset_processes[process].poll())
                for asset_id, proc in self.asset_processes.items():
                    rc = proc.poll()
                    if rc is not None:
                        logger.error(f"Asset {asset_id} exited with return code {rc}")
            except Exception as e:
                logger.exception("Error in process_status_check_loop")
            
                
    async def run(self):
        self.mbus = NATS()
        await self.mbus.connect("nats://natsio:4222")

        self.start_processes()

        # await self.mbus.subscribe(f"{self.config['usg_id']}.admin", cb=self.admin_msg_handler)
        # await self.mbus.subscribe(f"{self.config['usg_id']}.control", cb=self.control_msg_handler)
        # await mbus.subscribe(f"*.telemetry.>", cb=self.telemetry_msg_handler)
        # await self.mbus.subscribe(">", cb=self.telemetry_msg_handler)
        await self.mbus.subscribe("logs.>", cb=self.logs_msg_handler)

        # periodically check status of processes (each second)
        asyncio.create_task(self.process_status_check_loop())

        await self.process_queue()


    def shutdown(self):
        for p in self.asset_processes:
            logger.debug(f"Terminating process {p.key}")
            p.terminate()
        # terminating ECMP process
        self.ecmp_process.terminate()


if __name__ == "__main__":
    # logger.debug(os.listdir("/code"))
    manager = ProcessManager("code/config/config.yaml")
    try:
        asyncio.run(manager.run())
        # manager.start_processes()
        input("Press Enter to shut down...\n")
    finally:
        manager.shutdown()



