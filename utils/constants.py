from snapshotter.utils.default_logger import logger
from snapshotter.utils.file_utils import read_json_file
from snapshotter.utils.rpc import RpcHelper
from web3 import constants as web3_constants
from web3 import Web3

from ..settings.config import settings as worker_settings

constants_logger = logger.bind(module='PowerLoom|BaseSnapshots|Constants')
# Getting current node

rpc_helper = RpcHelper()
current_node = rpc_helper.get_current_node()

