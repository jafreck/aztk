import logging
import sys

log = logging.getLogger("aztk.node-agent")

DEFAULT_FORMAT = "%(message)s"
VERBOSE_FORMAT = "[%(asctime)s] [%(filename)s:%(module)s:%(funcName)s:%(lineno)d] %(levelname)s - %(message)s"


def setup_logging():
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    log.setLevel(logging.INFO)
    logging.basicConfig(stream=sys.stdout, format=VERBOSE_FORMAT)
