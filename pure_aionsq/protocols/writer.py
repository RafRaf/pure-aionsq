from pure_aionsq.command import Command, CommandType
from pure_aionsq.protocols.base import BaseProtocol


class WriterProtocol(BaseProtocol):
    async def publish(self, topic: bytes, data: bytes):
        command = Command(CommandType.publish)
        self.transport.write(command.get_message(topic, payload=data))
