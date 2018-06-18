import logging

from pure_aionsq.command import Command, CommandType
from pure_aionsq.protocols.base import BaseProtocol
from pure_aionsq.settings import loggerName

logger = logging.getLogger(loggerName)


class WriterProtocol(BaseProtocol):
    async def publish(self, topic: bytes, data: bytes):
        command = Command(CommandType.publish)
        self.transport.write(command.gen_command(topic, payload=data))
