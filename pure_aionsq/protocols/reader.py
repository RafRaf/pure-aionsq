import asyncio
import logging

from pure_aionsq.command import Command, CommandType
from pure_aionsq.message import FrameType, Message
from pure_aionsq.protocols.base import BaseProtocol


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

            # Make a command to finish or requeue messages
            #
            if task.result():
                command = Command(CommandType.finish)
                transport.write(command.get_message(message_id))
            else:
                command = Command(CommandType.requeue)
                transport.write(command.get_message(message_id, '30'))
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
        logging.warning('Data received: {}, {}'.format(frame_type, message_data))

        if frame_type == FrameType.message:
            task = asyncio.ensure_future(self.message_handler(message_data), loop=self.loop)

            # Add finalisation callback (finish or requeue)
            #
            task.add_done_callback(self._finish_callback(self.transport, message.message_id))
        elif frame_type == FrameType.response:
            if message.is_heartbeat:
                self.transport.write(Command(CommandType.nop).get_message())
            # else:
            #     rdy = Command(CommandType.ready).get_message('1')
            #     self.transport.write(rdy)
            #     logging.warning(rdy)
        elif frame_type == FrameType.error:
            logging.info(message_data)

        # Ready for a next message
        #
        rdy = Command(CommandType.ready).get_message('1')
        self.transport.write(rdy)
        logging.warning(rdy)
