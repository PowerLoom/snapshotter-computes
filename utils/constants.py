from snapshotter.utils.default_logger import logger
from snapshotter.utils.file_utils import read_json_file
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from ..settings.config import settings as worker_settings

constants_logger = logger.bind(module='PowerLoom|Aave|Constants')
# Getting current node

rpc_helper = RpcHelper()
current_node = rpc_helper.get_current_node()

# LOAD AAVE ABIs
pool_contract_abi = read_json_file(
    worker_settings.aave_contract_abis.pool_contract,
    constants_logger,
)

pool_data_provider_abi = read_json_file(
    worker_settings.aave_contract_abis.pool_data_provider_contract,
    constants_logger,
)

aave_oracle_abi = read_json_file(
    worker_settings.aave_contract_abis.aave_oracle,
    constants_logger,
)

erc20_abi = read_json_file(
    worker_settings.aave_contract_abis.erc20,
    constants_logger,
)

a_token_abi = read_json_file(
    worker_settings.aave_contract_abis.a_token,
    constants_logger,
)

# Init Aave V3 Core contract Objects
pool_contract_obj = current_node['web3_client'].eth.contract(
    address=Web3.toChecksumAddress(
        worker_settings.contract_addresses.aave_v3_pool,
    ),
    abi=pool_contract_abi,
)

pool_data_provider_contract_obj = current_node['web3_client'].eth.contract(
    address=Web3.toChecksumAddress(
        worker_settings.contract_addresses.pool_data_provider,
    ),
    abi=pool_data_provider_abi,
)

aave_oracle_contract_obj = current_node['web3_client'].eth.contract(
    address=Web3.toChecksumAddress(
        worker_settings.contract_addresses.aave_oracle,
    ),
    abi=aave_oracle_abi,
)

# FUNCTION SIGNATURES and OTHER CONSTANTS
AAVE_EVENT_SIGS = {
    'Withdraw': 'Withdraw(address,address,address,uint256)',
    'Supply': 'Supply(address,address,address,uint256,uint16)',
    'LiquidationCall': 'LiquidationCall(address,address,address,uint256,uint256,address,bool)',
    'ReserveDataUpdated': 'ReserveDataUpdated(address,uint256,uint256,uint256,uint256,uint256)',
    'Borrow': 'Borrow(address,address,address,uint256,uint8,uint256,uint16)',
}
AAVE_EVENTS_ABI = {
    'Withdraw': pool_contract_obj.events.Withdraw._get_event_abi(),
    'Supply': pool_contract_obj.events.Supply._get_event_abi(),
    'LiquidationCall': pool_contract_obj.events.LiquidationCall._get_event_abi(),
    'ReserveDataUpdated': pool_contract_obj.events.ReserveDataUpdated._get_event_abi(),
    'Borrow': pool_contract_obj.events.Borrow._get_event_abi(),
}
AAVE_CORE_EVENTS = ('Withdraw', 'Supply', 'Borrow', 'Repay')
tokens_decimals = {
    'USDT': 6,
    'DAI': 18,
    'USDC': 6,
    'WETH': 18,
}

ray = 1e27
seconds_in_year = 31536000
