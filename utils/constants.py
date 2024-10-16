"""
This module contains constants and initializations for the Uniswap-related computations.
It sets up contract objects, loads ABIs, and defines various constants used throughout the project.
"""

from snapshotter.utils.default_logger import logger
from snapshotter.utils.file_utils import read_json_file
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from ..settings.config import settings as worker_settings

# Maximum gas limit for static calls
max_gas_static_call = 30_000_000_000

# Uniswap V3 tick range
MIN_TICK = int(-887272)
MAX_TICK = -MIN_TICK

# Zero address constant
ZER0_ADDRESS = str('0x' + '0' * 40)

# Uniswap V3 fee divisor
UNISWAPV3_FEE_DIV = int(1000000)

# Set up logger for this module
constants_logger = logger.bind(module='PowerLoom|Uniswap|Constants')

# Bytecode for Uniswap V3 helper contract
# https://github.com/getjiggy/evm-helpers
univ3_helper_bytecode_json = read_json_file(
    'computes/static/bytecode/univ3_helper.json',
    constants_logger,
)
univ3_helper_bytecode = univ3_helper_bytecode_json['bytecode']

# Initialize RPC helper and get current node
rpc_helper = RpcHelper()
rpc_helper.sync_init()
current_node = rpc_helper.get_current_node()

# Load contract ABIs
pair_contract_abi = read_json_file(
    worker_settings.uniswap_contract_abis.pair_contract,
    constants_logger,
)
erc20_abi = read_json_file(
    worker_settings.uniswap_contract_abis.erc20,
    constants_logger,
)
router_contract_abi = read_json_file(
    worker_settings.uniswap_contract_abis.router,
    constants_logger,
)
uniswap_trade_events_abi = read_json_file(
    worker_settings.uniswap_contract_abis.trade_events,
    constants_logger,
)
factory_contract_abi = read_json_file(
    worker_settings.uniswap_contract_abis.factory,
    constants_logger,
)

# Load helper contract ABI
helper_contract_abi = read_json_file(
    'computes/static/abis/UniV3Helper.json',
    constants_logger,
)

# Override address for helper contract
override_address = Web3.to_checksum_address('0x' + '1' * 40)

# Initialize helper contract
helper_contract = current_node['web3_client'].eth.contract(
    address=Web3.to_checksum_address(
        override_address,
    ), abi=helper_contract_abi,
)

factory_contract_obj = current_node['web3_client'].eth.contract(
    address=Web3.to_checksum_address(
        worker_settings.contract_addresses.uniswap_v3_factory,
    ),
    abi=factory_contract_abi,
)
pool_contract_obj = current_node['web3_client'].eth.contract(
    address=Web3.to_checksum_address(
        '0x' + '1' * 40,  # Placeholder address for getting event ABIs
    ),
    abi=pair_contract_abi,
)

# Define Uniswap trade event signatures
UNISWAP_TRADE_EVENT_SIGS = {
    'Swap': 'Swap(address,address,int256,int256,uint160,uint128,int24)',
    'Mint': 'Mint(address,address,int24,int24,uint128,uint256,uint256)',
    'Burn': 'Burn(address,int24,int24,uint128,uint256,uint256)',
}

# Define Uniswap event ABIs
UNISWAP_EVENTS_ABI = {
    'Swap': pool_contract_obj.events.Swap._get_event_abi(),
    'Mint': pool_contract_obj.events.Mint._get_event_abi(),
    'Burn': pool_contract_obj.events.Burn._get_event_abi(),
}

# Define token decimals for common tokens
TOKENS_DECIMALS = {
    worker_settings.contract_addresses.USDT: 6,
    worker_settings.contract_addresses.DAI: 18,
    worker_settings.contract_addresses.USDC: 6,
    worker_settings.contract_addresses.WETH: 18,
}

# List of stable tokens
STABLE_TOKENS_LIST = [
    worker_settings.contract_addresses.USDC,
    worker_settings.contract_addresses.USDT,
    worker_settings.contract_addresses.DAI,
]
