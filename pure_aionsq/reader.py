class Reader:
    def __init__(
            self,
            topic,
            channel,
            message_handler,
            nsqd_tcp_addresses,
    ):
        self.topic = topic
        self.channel = channel
        self.message_handler = message_handler
        self.nsqd_tcp_addresses = nsqd_tcp_addresses
