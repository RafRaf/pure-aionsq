import aiohttp
import asyncio

from pure_aionsq.log import logger
from pure_aionsq.command import Command, CommandType
from pure_aionsq.protocols.reader import ReaderProtocol


class NSQ:
    def __init__(self, lookupd=None, lookupd_heartbeat=5):
        self.loop = None
        self.readers = list()
        self.lookup = lookupd
        self.lookupd_heartbeat = lookupd_heartbeat
        self.connections = dict()

    async def shutdown(self):
        logger.debug('Shutdown the application..')

        self.loop.shutdown_asyncgens()

    def register_reader(self, reader):
        """

        :param reader:
        :return:
        """
        self.readers.append(reader)

    def _get_protocol_class(self, reader):
        return lambda: ReaderProtocol(self.loop, reader.message_handler)

    def _create_connection(self, reader, address, port=4150):
        yield self.loop.create_connection(
            self._get_protocol_class(reader),
            address,
            port,
        )

    async def _get_lookupd_data(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(f'{self.lookup}/nodes') as response:
                if response.status == 200:
                    raw_data = await response.json()
                    result = [(item['broadcast_address'], item['http_port']) for item in raw_data['producers']]
                else:
                    result = []
        return result

    async def dispatcher(self):
        command = Command(CommandType.subscribe)

        while True:
            new_connections = await self._get_lookupd_data()

            for connection in new_connections:
                hostname, port = connection

                if connection not in self.connections:
                    for reader in self.readers:
                        transport, protocol = await self.loop.create_connection(
                            self._get_protocol_class(reader),
                            hostname,
                            port,
                        )
                        self.connections[connection] = (transport, protocol)
                        message = command.get_message(reader.topic, reader.channel)
                        transport.write(message)

            logger.debug(f'*** {new_connections} ***')

            await asyncio.sleep(self.lookupd_heartbeat)

            # Check for closing connections ****
            #
            self.connections = {
                addr: item
                for addr, item in self.connections.items()
                if not item[0].is_closing()
            }

    def run(self):
        """

        :return:
        """
        self.loop = asyncio.new_event_loop()

        logger.debug('The Application is running..')

        try:
            self.loop.run_until_complete(self.dispatcher())
        finally:
            self.loop.run_until_complete(self.shutdown())
            self.loop.close()
