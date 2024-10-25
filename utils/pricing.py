from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from computes.settings.config import settings as worker_settings
from computes.utils.constants import aave_oracle_abi
from computes.utils.constants import pool_contract_obj

pricing_logger = logger.bind(module='PowerLoom|Aave|Pricing')


async def get_asset_price_in_block_range(
    asset_metadata,
    from_block,
    to_block,
    rpc_helper: RpcHelper,
    debug_log=True,
):
    """
    Retrieves the price of a token for a given block range.

    Args:
        asset_metadata (dict): Metadata of the asset.
        from_block (int): Starting block number.
        to_block (int): Ending block number.
        rpc_helper (RpcHelper): RPC helper object.
        debug_log (bool): Flag to enable debug logging.

    Returns:
        dict: A dictionary mapping block numbers to asset prices.
    """
    try:
        asset_price_dict = dict()
        asset_address = Web3.to_checksum_address(asset_metadata['address'])
        
        abi_dict = get_contract_abi_dict(
            abi=aave_oracle_abi,
        )

        asset_usd_quote = await rpc_helper.batch_eth_call_on_block_range(
            abi_dict=abi_dict,
            contract_address=worker_settings.contract_addresses.aave_oracle,
            from_block=from_block,
            to_block=to_block,
            function_name='getAssetPrice',
            params=[asset_address],
        )

        # Convert prices to 8 decimal format
        asset_usd_quote = [(quote[0] * (10 ** -8)) for quote in asset_usd_quote]
        for i, block_num in enumerate(range(from_block, to_block + 1)):
            asset_price_dict[block_num] = asset_usd_quote[i]

        if debug_log:
            pricing_logger.debug(
                f"{asset_metadata['symbol']}: usd price is {asset_price_dict}",
            )

        return asset_price_dict

    except Exception as err:
        pricing_logger.opt(exception=True, lazy=True).trace(
            (
                'Error while calculating price of asset:'
                f" {asset_metadata['symbol']} | {asset_metadata['address']}|"
                ' err: {err}'
            ),
            err=lambda: str(err),
        )
        raise err


async def get_all_asset_prices(
    from_block,
    to_block,
    rpc_helper: RpcHelper,
    debug_log=True,
):
    """
    Retrieves prices for all assets in the Aave pool for a given block range.

    Args:
        from_block (int): Starting block number.
        to_block (int): Ending block number.
        rpc_helper (RpcHelper): RPC helper object.
        debug_log (bool): Flag to enable debug logging.

    Returns:
        dict: A dictionary mapping block numbers to dictionaries of asset prices.
    """
    try:
        # Fetch asset list from the pool contract
        [asset_list] = await rpc_helper.web3_call(
            tasks=[pool_contract_obj.functions.getReservesList()],
        )

        abi_dict = get_contract_abi_dict(
            abi=aave_oracle_abi,
        )

        # get all asset prices in the block range from the Aave Oracle contract
        # https://docs.aave.com/developers/core-contracts/aaveoracle
        asset_prices_bulk = await rpc_helper.batch_eth_call_on_block_range(
            abi_dict=abi_dict,
            contract_address=worker_settings.contract_addresses.aave_oracle,
            from_block=from_block,
            to_block=to_block,
            function_name='getAssetsPrices',
            params=[asset_list],
        )

        if debug_log:
            pricing_logger.debug(
                f'Retrieved bulk prices for aave assets: {asset_prices_bulk}',
            )

        # Organize prices by block number and asset address
        all_assets_price_dict = {block_num: {} for block_num in range(from_block, to_block + 1)}

        for i, block_num in enumerate(range(from_block, to_block + 1)):
            matches = zip(asset_list, asset_prices_bulk[i][0])

            for match in matches:
                all_assets_price_dict[block_num][match[0]] = match[1]

        return all_assets_price_dict

    except Exception as err:
        pricing_logger.opt(exception=True, lazy=True).trace(
            (
                'Error while calculating bulk asset prices:'
                ' err: {err}'
            ),
            err=lambda: str(err),
        )
        raise err
