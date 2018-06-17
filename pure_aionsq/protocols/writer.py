import logging

from command import Command, CommandType
from protocols.base import BaseProtocol
from settings import loggerName

logger = logging.getLogger(loggerName)


class WriterProtocol(BaseProtocol):
    async def publish(self, topic: bytes, data: bytes):
        command = Command(CommandType.publish)
        self.transport.write(command.gen_command(topic, payload=data))
