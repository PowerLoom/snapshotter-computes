"""
This module contains constants for Uniswap-related computations.
It loads ABIs from JSON files and defines constants. No RPC initialization at import.
"""

from snapshotter.utils.default_logger import logger
from snapshotter.utils.file_utils import read_json_file
from computes.settings.config import settings as worker_settings

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

# Load contract ABIs (file-load only, no RPC)
pair_contract_abi = read_json_file(
    worker_settings.uniswap_contract_abis.pair_contract,
    constants_logger,
)
erc20_abi = read_json_file(
    worker_settings.uniswap_contract_abis.erc20,
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
    worker_settings.uniswap_contract_abis.uniswap_v3_helper,
    constants_logger,
)

# Define Uniswap trade event signatures
UNISWAP_TRADE_EVENT_SIGS = {
    'Swap': 'Swap(address,address,int256,int256,uint160,uint128,int24)',
    'Mint': 'Mint(address,address,int24,int24,uint128,uint256,uint256)',
    'Burn': 'Burn(address,int24,int24,uint128,uint256,uint256)',
}

# Define Uniswap event ABIs (from JSON, no Web3 contract needed)
UNISWAP_EVENTS_ABI = uniswap_trade_events_abi

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

# Minimal ABI for pool verification
POOL_ABI = [
    {
        "inputs": [],
        "name": "factory",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "token0",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "token1",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "fee",
        "outputs": [{"internalType": "uint24", "name": "", "type": "uint24"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "tickSpacing",
        "outputs": [{"internalType": "int24", "name": "", "type": "int24"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "slot0",
        "outputs": [
            {"internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"},
            {"internalType": "int24", "name": "tick", "type": "int24"},
            {"internalType": "uint16", "name": "observationIndex", "type": "uint16"},
            {"internalType": "uint16", "name": "observationCardinality", "type": "uint16"},
            {"internalType": "uint16", "name": "observationCardinalityNext", "type": "uint16"},
            {"internalType": "uint8", "name": "feeProtocol", "type": "uint8"},
            {"internalType": "bool", "name": "unlocked", "type": "bool"}
        ],
        "stateMutability": "view",
        "type": "function"
    }
]

# ERC20 ABI for token verification
ERC20_ABI = [
    {
        "inputs": [],
        "name": "name",
        "outputs": [{"internalType": "string", "name": "", "type": "string"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "symbol",
        "outputs": [{"internalType": "string", "name": "", "type": "string"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function"
    }
]
