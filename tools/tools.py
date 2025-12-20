from tools.logger import get_logger
import aiohttp
import ipaddress
from dataclasses import dataclass


logger = get_logger(__name__)


@dataclass
class RedisNode:
    host: str
    port: int


async def async_send_slack_notification(uri: str, message: str) -> bool:
    payload = {"text": message}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                    uri,
                    json=payload,
                    headers={'Content-Type': 'application/json'},
                    timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status != 200:
                    text = await response.text()
                    raise Exception(f"status code: {response.status}, response: {text}")
                logger.info('successfully send slack notification')
                return True
    except Exception as e:
        logger.error('failed to send slack notification %s', str(e))
        raise e


async def async_send_gotify_notification(uri: str, title: str, message: str) -> bool:
    payload = {"title": title, "message": message}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                    uri,
                    json=payload,
                    headers={'Content-Type': 'application/json'},
                    timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status != 200:
                    text = await response.text()
                    raise Exception(f"status code: {response.status}, response: {text}")
                logger.info('successfully send gotify notification')
                return True
    except Exception as e:
        logger.error('failed to send gotify notification %s', str(e))
        raise e


def pars_nodes(nodes: str) -> list[RedisNode]:
    result = list()
    nodes_split = nodes.split(',')
    if len(nodes_split) != 6:
        raise Exception('there is no 6 node')
    for node in nodes_split:
        try:
            node_split = node.split(sep=':', maxsplit=1)
            assert len(node_split) == 2
            ipaddress.ip_address(node_split[0])
            assert 0 <= int(node_split[1]) <= 65535
            result.append(RedisNode(host=node_split[0], port=int(node_split[1])))
        except:
            logger.error('invalid node %s', node)
            raise Exception('invalid node {0}'.format(node))
    return result