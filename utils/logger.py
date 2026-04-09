import logging
import sys


class FlushStreamHandler(logging.StreamHandler):

    def emit(self, record: logging.LogRecord) -> None:
        super().emit(record)
        self.flush()


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = FlushStreamHandler(sys.stdout)
    
        handler.setFormatter(logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s"
        ))

        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False

    return logger
