from pure_aionsq import log
from pure_aionsq.nsq import NSQ
from pure_aionsq.reader import Reader


async def handler(message: bytes):
    log.warning("I've got a message: %s", message.decode())
    return True

if __name__ == '__main__':
    nsq = NSQ(lookupd='http://nsqlookupd:4161')

    reader = Reader(
        topic='test_topic', channel='test_channel',
        # nsqd_tcp_addresses=['nsqd'],
        message_handler=handler,
    )

    # Register all readers
    #
    nsq.register_reader(reader)
    # ...

    # Run the event loop
    #
    nsq.run()
