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

erc721_abi = read_json_file(
    worker_settings.contract_abis.erc721,
    constants_logger,
)

erc721_contract_object = current_node['web3_client'].eth.contract(
    abi=erc721_abi,
)

# FUNCTION SIGNATURES and OTHER CONSTANTS
ERC721_EVENT_SIGS = {
    'Transfer': 'Transfer(address,address,uint256)',
}

ERC721_EVENTS_ABI = {
    'Transfer': erc721_contract_object.events.Transfer._get_event_abi(),
}

ZERO_ADDRESS = '0x' + '0' * 40