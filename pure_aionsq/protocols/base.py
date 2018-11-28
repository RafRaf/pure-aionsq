import asyncio

from pure_aionsq.log import logger


class BaseProtocol(asyncio.Protocol):
    def __init__(self, loop):
        self.loop = loop
        self.transport = None

    def connection_made(self, transport):
        """

        :param transport:
        :return:
        """
        self.transport = transport

        transport.write(b'  V2')
        logger.info('Connection established')

    def connection_lost(self, exc):
        """

        :param exc:
        :return:
        """
        logger.info('Connection lost')
