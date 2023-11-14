from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import RpcHelper

constants_logger = logger.bind(module='Powerloom|Boost|Constants')
# Getting current node

rpc_helper = RpcHelper()
current_node = rpc_helper.get_current_node()
