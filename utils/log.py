from constants import *
import logging
import sys

# Logger to use everywhere
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(LOGGER)
