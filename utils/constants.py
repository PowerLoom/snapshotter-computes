"""
This module contains constants and initializations for the Uniswap-related computations.
It sets up contract objects, loads ABIs, and defines various constants used throughout the project.
"""

from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import logger
from snapshotter.utils.file_utils import read_json_file
from rpc_helper.rpc import RpcHelper
from web3 import Web3
import asyncio
import threading
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

# Initialize RPC helper and get current node
# Initialize RPC helper and get current node
rpc_helper = RpcHelper(settings.rpc)
_rpc_initialized = False
_rpc_init_lock = threading.Lock() # Lock to prevent race conditions if imported concurrently


async def _async_init_rpc():
    """Internal async function to initialize RpcHelper."""
    global _rpc_initialized
    # Ensure initialization happens only once
    if not _rpc_initialized:
        with _rpc_init_lock:
            if not _rpc_initialized:
                constants_logger.info("Initializing RpcHelper...")
                await rpc_helper.init()
                _rpc_initialized = True
                constants_logger.info("RpcHelper initialization complete.")
            else:
                constants_logger.debug("RpcHelper already initialized by another thread/import.")
    else:
        constants_logger.debug("RpcHelper already initialized.")


def _run_init_in_new_loop():
    """Runs the async init function in a new event loop."""
    try:
        asyncio.run(_async_init_rpc())
    except Exception:
        constants_logger.exception("Exception during RpcHelper initialization in new loop.")
        # Ensure flag is not set if init failed
        global _rpc_initialized
        _rpc_initialized = False


# --- Initialization Logic ---
# Check if already initialized (e.g., by a concurrent import) before proceeding
if not _rpc_initialized:
    try:
        # Try to get the running event loop
        loop = asyncio.get_running_loop()
        constants_logger.debug(f"Detected running event loop: {loop}")

        # If a loop is running, we cannot block it directly.
        # Run the initialization in a separate thread using a new loop via asyncio.run().
        # This blocks the *current* (importing) thread until init is done,
        # without interfering with the already running event loop.
        constants_logger.info("Running loop detected. Initializing RPC in separate thread.")
        init_thread = threading.Thread(target=_run_init_in_new_loop, daemon=True)
        init_thread.start()
        init_thread.join() # Wait for the initialization thread to complete

    except RuntimeError:
        # No event loop is running in this thread.
        constants_logger.info("No running loop detected. Initializing RPC synchronously.")
        # Run the initialization directly using asyncio.run().
        _run_init_in_new_loop()

# --- Post-Initialization ---
# Check if initialization was successful
if not _rpc_initialized:
    # Log error and raise, as subsequent code depends on this.
    error_msg = "RPC Helper failed to initialize. Cannot proceed."
    constants_logger.error(error_msg)
    raise RuntimeError(error_msg)

current_node = rpc_helper.get_current_node()

if not current_node:
    # This might happen if init() succeeded but get_current_node() failed.
    error_msg = "Failed to get current_node after RPC initialization."
    constants_logger.error(error_msg)
    raise RuntimeError(error_msg)

# Load contract ABIs
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

# Initialize helper contract
helper_contract = current_node['web3_client'].eth.contract(
    address=Web3.to_checksum_address(
        worker_settings.contract_addresses.uniswap_v3_helper,
    ),
    abi=helper_contract_abi,
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
