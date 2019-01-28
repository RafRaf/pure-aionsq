import struct
from enum import Enum
from typing import Union


class CommandType(Enum):
    identify = b'IDENTIFY'
    subscribe = b'SUB'
    publish = b'PUB'
    multiple_publish = b'MPUB'
    deferred_publish = b'DPUB'
    ready = b'RDY'
    finish = b'FIN'
    requeue = b'REQ'
    touch = b'TOUCH'
    close = b'CLS'
    nop = b'NOP'
    authenticate = b'AUTH'


class Command:
    def __init__(self, command_type: CommandType):
        self.command_type = command_type

    def _converter(self, value: Union[str, bytes, int]) -> bytes:
        """

        :param value:
        :return:
        """
        if isinstance(value, str):
            return value.encode()
        elif isinstance(value, int):
            return str(value).encode()
        return value

    def get_message(self, *params: Union[str, bytes], payload: Union[str, bytes]=None) -> bytes:
        """

        :param params: arguments like topic/channel
        :param payload:
        :return:
        """
        if payload:
            length = len(payload)
            data = struct.Struct(f'>l{length}s').pack(length, self._converter(payload))
        else:
            data = None

        # Construct a base message signature
        #
        params_data = b' '.join((self._converter(param) for param in params))
        cmd = bytearray(self.command_type.value + b' ' + params_data + b'\n')

        return cmd + data if data else cmd
