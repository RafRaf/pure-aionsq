class RequeueException(Exception):
    pass


class ConnectionClosedException(Exception):
    def __init__(self, reader):
        super().__init__()
        self.reader = reader
