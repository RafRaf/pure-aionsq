import logging

from pure_aionsq.settings import loggerName


_logger = logging.getLogger(loggerName)
_logger.setLevel(logging.DEBUG)
logger = _logger
