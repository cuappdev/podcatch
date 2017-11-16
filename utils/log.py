import sys
import logging
import utils.constants as constants

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(constants.LOGGER)
