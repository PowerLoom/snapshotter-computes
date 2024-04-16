import math

from snapshotter.utils.default_logger import logger


helper_logger = logger.bind(module='PowerLoom|NftDataSnapshots|Helpers')


def truncate(number, decimals=5):
    """
    Returns a value truncated to a specific number of decimal places.
    """
    if not isinstance(decimals, int):
        raise TypeError('decimal places must be an integer.')
    elif decimals < 0:
        raise ValueError('decimal places has to be 0 or more.')
    elif decimals == 0:
        return math.trunc(number)

    factor = 10.0 ** decimals
    return math.trunc(number * factor) / factor
