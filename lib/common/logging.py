import logging

__all__ = ['logger']

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", datefmt="%Y-%m-%d %I:%M:%S %p")
logger = logging.getLogger("pyflink-logger")
logger.setLevel(logging.INFO)