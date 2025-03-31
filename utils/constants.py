"""
This module contains constants and initializations for Uniswap-related operations.

It includes ABI loading, contract object initializations, and various constants
used throughout the Uniswap interaction processes.
"""

from web3 import Web3

from computes.settings.config import settings as worker_settings
from snapshotter.utils.default_logger import logger
from snapshotter.utils.file_utils import read_json_file
from snapshotter.utils.rpc import RpcHelper
import asyncio
import threading

# Initialize logger for this module
constants_logger = logger.bind(module='PowerLoom|Uniswap|Constants')

# Initialize RPC helper and get current node
rpc_helper = RpcHelper()
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

constants_logger.info(f"RpcHelper ready. Current node URL: {current_node.get('url', 'N/A')}")

# Load ABIs for various contracts
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

# Initialize Uniswap V2 Core contract objects
router_contract_obj = current_node['web3_client'].eth.contract(
    address=Web3.to_checksum_address(
        worker_settings.contract_addresses.iuniswap_v2_router,
    ),
    abi=router_contract_abi,
)
factory_contract_obj = current_node['web3_client'].eth.contract(
    address=Web3.to_checksum_address(
        worker_settings.contract_addresses.iuniswap_v2_factory,
    ),
    abi=factory_contract_abi,
)
dai_eth_contract_obj = current_node['web3_client'].eth.contract(
    address=Web3.to_checksum_address(
        worker_settings.contract_addresses.DAI_WETH_PAIR,
    ),
    abi=pair_contract_abi,
)
usdc_eth_contract_obj = current_node['web3_client'].eth.contract(
    address=Web3.to_checksum_address(
        worker_settings.contract_addresses.USDC_WETH_PAIR,
    ),
    abi=pair_contract_abi,
)
eth_usdt_contract_obj = current_node['web3_client'].eth.contract(
    address=Web3.to_checksum_address(
        worker_settings.contract_addresses.USDT_WETH_PAIR,
    ),
    abi=pair_contract_abi,
)

# Define function signatures and other constants

# Event signatures for Uniswap trade events
UNISWAP_TRADE_EVENT_SIGS = {
    'Swap': 'Swap(address,uint256,uint256,uint256,uint256,address)',
    'Mint': 'Mint(address,uint256,uint256)',
    'Burn': 'Burn(address,uint256,uint256,address)',
}

# ABI for Uniswap events
UNISWAP_EVENTS_ABI = {
    'Swap': usdc_eth_contract_obj.events.Swap._get_event_abi(),
    'Mint': usdc_eth_contract_obj.events.Mint._get_event_abi(),
    'Burn': usdc_eth_contract_obj.events.Burn._get_event_abi(),
}

# Token decimals for common tokens
tokens_decimals = {
    'USDT': 6,
    'DAI': 18,
    'USDC': 6,
    'WETH': 18,
}

