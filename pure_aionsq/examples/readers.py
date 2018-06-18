import logging
from pure_aionsq.nsq import NSQ
from pure_aionsq.reader import Reader

from pure_aionsq.settings import loggerName

logger = logging.getLogger(loggerName)
logger.setLevel(logging.DEBUG)


async def handler(message: bytes):
    logger.warning("I've got a message: %s", message.decode())
    return True

if __name__ == '__main__':
    nsq = NSQ()

    reader = Reader(
        topic='test_topic', channel='test_channel',
        nsqd_tcp_addresses=['nsqd'],
        message_handler=handler,
    )

    # Register all readers
    #
    nsq.register_reader(reader)
    # ...

    # Run the event loop
    #
    nsq.run()
