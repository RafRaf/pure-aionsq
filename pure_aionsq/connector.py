from functools import partial


class Connection:
    def __init__(self, transport, protocol, nsqd_tcp_address, port):
        self.transport = transport
        self.protocol = protocol
        self.nsqd_tcp_address = nsqd_tcp_address
        self.port = port

    @property
    def connection_string(self):
        return f'{self.nsqd_tcp_address}:{self.port}'

    def __hash__(self):
        return (self.nsqd_tcp_address, self.port).__hash__()


class Connector:
    connections = dict()

    def __init__(self, protocol_class, nsqd_tcp_addresses):
        self.protocol_class = protocol_class
        self.nsqd_tcp_addresses = nsqd_tcp_addresses
        # self._constructor(**kwargs)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def _get_host_port(self, raw_address: str):
        parts = raw_address.split(':')

        if parts == 1:
            return parts[0], '4150'  # default nsqd port (tcp)
        return parts[0], parts[1]

    def connect(self):
        for nsqd_tcp_address in self.nsqd_tcp_addresses:
            host, port = self._get_host_port(nsqd_tcp_address)
            protocol = lambda: self.protocol_class(self.loop)

            #
            #
            transport, protocol = yield from self.loop.create_connection(protocol, host, port,)

            #
            #
            connection = Connection(transport, protocol, nsqd_tcp_address, port)
            self.connections[connection.connection_string] = connection

    def disconnect(self):
        for connection in self.connections:
            connection.transport.close()

    def _constructor(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def connector_factory(protocol_class):
    return partial(Connector, protocol_class=protocol_class)
