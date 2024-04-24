from snapshotter.utils.default_logger import logger
from snapshotter.utils.file_utils import read_json_file
from snapshotter.utils.rpc import RpcHelper
from web3 import constants as web3_constants
from web3 import Web3

from ..settings.config import settings as worker_settings

constants_logger = logger.bind(module='PowerLoom|Aave|Constants')
# Getting current node

rpc_helper = RpcHelper()
rpc_helper.sync_init()
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

ui_pool_data_provider_abi = read_json_file(
    worker_settings.aave_contract_abis.ui_pool_data_provider,
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

stable_debt_token_abi = read_json_file(
    worker_settings.aave_contract_abis.stable_token,
    constants_logger,
)

vaiable_debt_token_abi = read_json_file(
    worker_settings.aave_contract_abis.variable_token,
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

ui_pool_data_provider_contract_obj = current_node['web3_client'].eth.contract(
    address=Web3.toChecksumAddress(
        worker_settings.contract_addresses.ui_pool_data_provider,
    ),
    abi=ui_pool_data_provider_abi,
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
    'Repay': 'Repay(address,address,address,uint256,bool)',
    'Borrow': 'Borrow(address,address,address,uint256,uint8,uint256,uint16)',
    'LiquidationCall': 'LiquidationCall(address,address,address,uint256,uint256,address,bool)',
}
AAVE_EVENTS_ABI = {
    'Withdraw': pool_contract_obj.events.Withdraw._get_event_abi(),
    'Supply': pool_contract_obj.events.Supply._get_event_abi(),
    'Repay': pool_contract_obj.events.Repay._get_event_abi(),
    'Borrow': pool_contract_obj.events.Borrow._get_event_abi(),
    'LiquidationCall': pool_contract_obj.events.LiquidationCall._get_event_abi(),
}
AAVE_CORE_EVENTS = ('Withdraw', 'Supply', 'Borrow', 'Repay')

# AAVE Base 27 format
RAY = 1000000000000000000000000000
HALF_RAY = 500000000000000000000000000

SECONDS_IN_YEAR = 31536000

# Decimal base for AAVE price oracle values
ORACLE_DECIMALS = 8

# Divisor for AAVE rate detail values
DETAILS_BASIS = 10000
