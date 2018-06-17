import asyncio
import logging

from connector import connector_factory
from protocols.writer import WriterProtocol

from settings import loggerName

logger = logging.getLogger(loggerName)


async def publish(topic: bytes, data: bytes):
    factory = connector_factory(protocol_class=WriterProtocol)

    with factory(nsqd_tcp_addresses=['nsqd:4150']) as connector:
        for connection in connector.connections.values():
            await connection.publish(topic, data)

    # while True:
    #     await asyncio.sleep(3)
    #     logger.error('Idle')


async def shutdown(loop):
    loop.shutdown_asyncgens()


if __name__ == '__main__':
    topic = b'topic_test'
    data = b'Hello world'

    loop = asyncio.new_event_loop()

    try:
        loop.run_until_complete(publish(topic, data))
    finally:
        loop.run_until_complete(shutdown(loop))
        loop.close()
