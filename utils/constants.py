from snapshotter.utils.default_logger import logger
from snapshotter.utils.file_utils import read_json_file
from snapshotter.utils.rpc import RpcHelper

from ..settings.config import settings as worker_settings

constants_logger = logger.bind(module='PowerLoom|StakingYieldSnapshots|Constants')
# Getting current node

rpc_helper = RpcHelper()
rpc_helper.sync_init()
current_node = rpc_helper.get_current_node()

aggregator_contract_abi = read_json_file(
    worker_settings.contract_abis.EACAggregatorProxy,
    constants_logger,
)

SECONDS_IN_DAY = 86400
