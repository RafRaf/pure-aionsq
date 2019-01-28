import struct
from enum import Enum
from typing import Tuple


class FrameType(Enum):
    response = 0
    error = 1
    message = 2


class Message:
    def __init__(self, data: bytes):
        self.is_heartbeat = False
        self._parse(data)

    def _parse(self, data: bytes):
        # Frame
        #
        length_offset = len(data) - 8
        frame_struct = struct.Struct(f'>ii{length_offset}s')
        _, self.frame_type, frame_data = frame_struct.unpack(data)

        # Recognize the message by `frame_type`
        #
        handlers = {
            frame_type.value: getattr(self, '_parse_%s' % frame_type.name)
            for frame_type in FrameType
        }

        response_handler = handlers.get(self.frame_type)

        if not response_handler:
            raise AssertionError('Wrong frame type %d', self.frame_type)

        # Process data by a right handler
        #
        response_handler(frame_data)

    def _parse_response(self, frame_data: bytes):
        if frame_data.startswith(b'OK'):
            self.message_body = 'OK'
        elif frame_data.startswith(b'_heartbeat_'):
            self.is_heartbeat = True
            self.message_body = '❤ Heartbeat ❤'
        else:
            self.message_body = 'Something'  # TODO: what is it?

    def _parse_message(self, frame_data: bytes):
        metadata_offset = len(frame_data) - 26
        message_struct = struct.Struct(f'>qh16s{metadata_offset}s')
        self.timestamp, self.attempts, self.message_id, self.message_body = message_struct.unpack(frame_data)

    def _parse_error(self, frame_data: bytes):
        metadata_offset = len(frame_data) - 2
        message_struct = struct.Struct(f'>qh{metadata_offset}s')
        self.timestamp, self.attempts, self.message_body = message_struct.unpack(frame_data)

    @property
    def msg(self) -> Tuple[FrameType, str]:
        return FrameType(self.frame_type), self.message_body
