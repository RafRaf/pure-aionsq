from pure_aionsq.exceptions import RequeueException


class Reader:
    def __init__(
            self,
            topic,
            channel,
            message_handler,
            nsqd_tcp_addresses=None,
    ):
        self.topic = topic
        self.channel = channel
        self.message_handler = message_handler  # self._message_handler_wrapper(message_handler)
        self.nsqd_tcp_addresses = nsqd_tcp_addresses

    # @staticmethod
    # def _message_handler_wrapper(handler):
    #     def wrapper(*args, **kwargs):
    #         try:
    #             handler(*args, **kwargs)
    #         except RequeueException:
    #             pass
    #     return wrapper
