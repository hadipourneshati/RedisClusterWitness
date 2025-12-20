import tools
import os
import configparser
import logging
import asyncio
import time
from redis.asyncio import Redis


class RedisNode:
    def __init__(self, host: str, port: int) -> None:
        self.host: str = host
        self.port: int = port
        self._reset_state()

    def _reset_state(self) -> None:
        self.role: str | None = None
        self.master_host: str | None = None
        self.master_port: int | None = None
        self.master: RedisNode | None = None
        self.link_status: str | None = None
        self.master_link_status: str | None = None

    def reset(self) -> None:
        """
        reset state of node
        set properties of:
            role
            master_host
            master_port
            master
            link_status
            master_link_status
        to None
        """
        self._reset_state()

    async def get_status(self):
        try:
            async with Redis(
                    host=self.host,
                    port=self.port,
                    timeout=5,
                    decode_responses=True
            ) as r:
                info = await r.info()
                role = info.get("role")
                if role in {"master", "slave"}:
                    self.role = role
                    self.link_status = "up"
                else:
                    logger.warning("role for node {0}:{1} is {2}".format(self.host, self.port, role))
                    self.role = "unknown"
                if role == "slave":
                    self.master_host = info.get("master_host")
                    self.master_port = int(info.get("master_port"))
                    self.master_link_status = info.get("master_link_status", "down")
        except Exception as e:
            logger.info("failed to connect to node {0}:{1} {2}".format(self.host, self.port, str(e)))
            self.link_status = "down"

    async def failover(self) -> bool:
        try:
            async with Redis(
                    host=self.host,
                    port=self.port,
                    timeout=5,
                    decode_responses=True
            ) as r:
                failover_result = await r.execute_command("CLUSTER FAILOVER TAKEOVER")
                failover_result = failover_result.decode('utf-8')
                if failover_result == 'OK':
                    return True
                else:
                    return False
        except Exception as e:
            logger.warning("failed to failover node {0}:{1} {2}".format(self.host, self.port, str(e)))
            return False

async def main():
    last_healthy_time = int(time.time())
    nodes = [RedisNode(host=n.host, port=n.port) for n in tools.pars_nodes(nodes=NODES)]
    while True:
        failover_candidates = list()
        for node in nodes:
            node.reset()
        await asyncio.gather(*[n.get_status() for n in nodes])

        # map masters to slaves
        for s in nodes:
            if s.role == "slave":
                for m in nodes:
                    if m.host == s.master_host and m.port == s.master_port:
                        s.master = m
                        break
        for slave in [n for n in nodes if n.role == "slave"]:
            if slave.master_link_status == "down":
                if slave.master.link_status == "down":
                    failover_candidates.append(slave)
        if len(failover_candidates) > 0:
            logger.info("failover need in {0} seconds".format(TIMEOUT - (int(time.time()) - last_healthy_time)))
            if int(time.time()) - last_healthy_time > TIMEOUT:
                failover_result = await asyncio.gather(*[f.failover() for f in failover_candidates])
                for i in zip(failover_candidates, failover_result):
                    if i[1]:
                        logger.info("successfully failover to {0}:{1} from {2}:{3}".format(i[0].host, i[0].port, i[0].master.host, i[0].master.port))
                    else:
                        logger.warning("failed to failover to {0}:{1} from {2}:{3}".format(i[0].host, i[0].port, i[0].master.host, i[0].master.port))
        else:
            logger.debug("nodes are ok.")
            last_healthy_time = int(time.time())
        time.sleep(CHECK_PERIOD)


if __name__ == '__main__':
    BASE_DIR = os.path.dirname(__file__)
    config = configparser.ConfigParser()
    config.read(os.path.join(BASE_DIR, "config.ini"))
    NODES = config.get(section='Default', option='nodes')
    CHECK_PERIOD = config.getint(section='Default', option='check_period', fallback=5)
    TIMEOUT = config.getint(section='Default', option='timeout', fallback=30)
    DEBUG = config.getboolean(section='Default', option='debug', fallback=False)
    SLACK = config.getboolean(section='Notification', option='slack', fallback=False)
    SLACK_URI = config.get(section='Notification', option='slack_uri')
    GOTIFY = config.getboolean(section='Notification', option='gotify', fallback=False)
    GOTIFY_URI = config.get(section='Notification', option='gotify_uri')
    LOG_PATH = os.path.join(BASE_DIR, 'RedisClusterWitness.log')
    logging.basicConfig(
        filename=LOG_PATH,
        filemode='a',
        level=logging.INFO,
        format='%(asctime)s\t%(levelname)s\t%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger = tools.get_logger('RedisClusterWitness')
    asyncio.run(main())
