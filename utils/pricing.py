import json

from web3 import Web3

from snapshotter.modules.computes.preloaders.eth_price.preloader import eth_price_preloader
from snapshotter.modules.computes.utils.helpers import get_token_eth_price_dict
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import RpcHelper

pricing_logger = logger.bind(module="PowerLoom|Uniswap|Pricing")

async def get_token_price_in_block_range(
    token_metadata: dict,
    from_block: int,
    to_block: int,
    rpc_helper: RpcHelper,
    debug_log: bool = True,
) -> dict:
    """
    Retrieves the price of a token for a given block range.

    Args:
        token_metadata (dict): Metadata of the token including address and decimals.
        from_block (int): Starting block number.
        to_block (int): Ending block number.
        rpc_helper (RpcHelper): RPC helper object for blockchain interactions.
        debug_log (bool, optional): Flag to enable debug logging. Defaults to True.

    Returns:
        dict: A dictionary mapping block numbers to token prices in USD.

    Raises:
        Exception: If there's an error during price calculation.
    """
    try:
        token_price_dict = dict()
        token_address = Web3.to_checksum_address(token_metadata["address"])
        token_decimals = int(token_metadata["decimals"])

        # Handle WETH separately
        if token_address == Web3.to_checksum_address(
            worker_settings.contract_addresses.WETH
        ):
            token_price_dict = await eth_price_preloader.get_eth_price_usd(
                from_block=from_block,
                to_block=to_block,
                rpc_helper=rpc_helper,
            )
        else:
            # Get token price in ETH
            token_eth_price_dict = await get_token_eth_price_dict(
                token_address=token_address,
                token_decimals=token_decimals,  
                from_block=from_block,
                to_block=to_block,
                rpc_helper=rpc_helper,
            )

            if token_eth_price_dict:
                # Get ETH price in USD
                eth_usd_price_dict = await eth_price_preloader.get_eth_price_usd(
                    from_block=from_block,
                    to_block=to_block,
                    rpc_helper=rpc_helper,
                )

                if debug_log:
                    pricing_logger.debug(
                        f"token_eth_price_dict: {token_eth_price_dict}"
                    )
                    pricing_logger.debug(
                        f"eth_usd_price_dict: {eth_usd_price_dict}"
                    )

                # Calculate token price in USD
                for block_num in range(from_block, to_block + 1):
                    token_price_dict[block_num] = token_eth_price_dict.get(
                        block_num,
                        0,
                    ) * (eth_usd_price_dict.get(block_num, 0))
            else:
                # Set price to 0 if no ETH price is available
                token_price_dict = {
                    block_num: 0 for block_num in range(from_block, to_block + 1)
                }
        
        if debug_log:
            pricing_logger.debug(
                f"{token_metadata['symbol']}: price is {token_price_dict}"
                f" | its eth price is {token_eth_price_dict}",
            )

        return token_price_dict

    except Exception as err:
        pricing_logger.opt(exception=True, lazy=True).trace(
            (
                "Error while calculating price of token:"
                f" {token_metadata['symbol']} | {token_metadata['address']}|"
                " err: {err}"
            ),
            err=lambda: str(err),
        )
        raise err
