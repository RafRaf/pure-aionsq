import asyncio
import logging

from command import Command, CommandType
from protocols.reader import ReaderProtocol

from settings import loggerName

logger = logging.getLogger(loggerName)


class NSQ:
    def __init__(self):
        self.loop = None
        self.readers = list()
        self.connections = dict()

    async def shutdown(self):
        logger.debug('Shutdown the application..')

        self.loop.shutdown_asyncgens()

    async def dispatcher(self):
        while True:
            await asyncio.sleep(3)
            logger.debug('Idle')

    def register_reader(self, reader):
        """

        :param reader:
        :return:
        """
        self.readers.append(reader)

    def _get_protocol(self, reader):
        return lambda: ReaderProtocol(self.loop, reader.message_handler)

    def _init_readers(self):
        """
        Initialize all registered readers
        """
        logger.warning(self.readers)

        command = Command(CommandType.subscribe)

        for reader in self.readers:
            for address in reader.nsqd_tcp_addresses:
                transport, protocol = yield from self.loop.create_connection(
                    self._get_protocol(reader),
                    address,
                    4150,
                )

                self.connections[address] = (transport, protocol)

                logger.debug(f'Registered reader (topic="{reader.topic}", channel="{reader.channel}")..')

                message = command.gen_command(reader.topic, reader.channel)
                transport.write(message)

    def run(self):
        """

        :return:
        """
        self.loop = asyncio.new_event_loop()

        logging.debug('The Application is running..')

        # Instantiate all connections
        #
        for reader in self._init_readers():
            self.loop.run_until_complete(reader)

        try:
            self.loop.run_until_complete(self.dispatcher())
        finally:
            self.loop.run_until_complete(self.shutdown())
            self.loop.close()
