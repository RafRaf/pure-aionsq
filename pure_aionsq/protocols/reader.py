import asyncio
import logging

from command import Command, CommandType
from message import Message, FrameType
from protocols.base import BaseProtocol

from settings import loggerName

logger = logging.getLogger(loggerName)


class ReaderProtocol(BaseProtocol):
    def __init__(self, loop, message_handler):
        super().__init__(loop)
        self.message_handler = message_handler

    def _finish_callback(self, transport, message_id: bytes):
        """

        :param transport:
        :param message_id:
        :return:
        """
        def callback(task):
            command_type = CommandType.finish if task.result() else CommandType.requeue

            # Make a command to finish or requeue messages
            #
            command = Command(command_type)
            transport.write(command.gen_command(message_id))
        return callback

    def data_received(self, data):
        """

        :param data:
        :return:
        """
        message = Message(data)
        frame_type, message_data = message.msg

        # We're have a new frame
        #
        logger.warning('Data received: {}, {}'.format(frame_type, message_data))

        if frame_type == FrameType.message:
            task = asyncio.ensure_future(self.message_handler(message_data), loop=self.loop)

            # Add finalisation callback (finish or requeue)
            #
            task.add_done_callback(self._finish_callback(self.transport, message.message_id))

            # Ready for a next message
            #
            self.transport.write(Command(CommandType.ready).gen_command('1'))
        elif frame_type == FrameType.response:
            if message.is_heartbeat:
                self.transport.write(Command(CommandType.nop).gen_command())
            # else:
            #     rdy = Command(CommandType.ready).gen_command('1')
            #     self.transport.write(rdy)
            #     logging.warning(rdy)
        elif frame_type == FrameType.error:
            logging.info(message_data)

        rdy = Command(CommandType.ready).gen_command('1')
        self.transport.write(rdy)
        logging.warning(rdy)