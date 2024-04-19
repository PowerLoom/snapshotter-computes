from snapshotter.utils.default_logger import logger
from snapshotter.utils.file_utils import read_json_file
from snapshotter.utils.rpc import RpcHelper

from ..settings.config import settings as worker_settings

constants_logger = logger.bind(module='PowerLoom|StakingYieldSnapshots|Constants')
# Getting current node

rpc_helper = RpcHelper()
current_node = rpc_helper.get_current_node()

lido_contract_abi = read_json_file(
    worker_settings.contract_abis.lido,
    constants_logger,
)

lido_contract_object = current_node['web3_client'].eth.contract(
    abi=lido_contract_abi,
    address=worker_settings.contract_addresses.lido,
)

LIDO_EVENTS_ABI = {
    'TokenRebased': lido_contract_object.events.TokenRebased._get_event_abi(),
}

LIDO_EVENTS_SIG = {
    'TokenRebased': 'TokenRebased(uint256,uint256,uint256,uint256,uint256,uint256,uint256)',
}

SECONDS_IN_YEAR = 31536000