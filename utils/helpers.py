import asyncio

from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import get_event_sig_and_abi
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from ..redis_keys import aave_asset_contract_data
from ..settings.config import settings as worker_settings
from .constants import AAVE_EVENT_SIGS
from .constants import AAVE_EVENTS_ABI
from .constants import current_node
from .constants import erc20_abi


helper_logger = logger.bind(module='PowerLoom|Aave|Helpers')


async def get_asset_metadata(
    asset_address: str,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
):
    try:
        asset_address = Web3.toChecksumAddress(asset_address)

        # check if cache exist
        asset_data_cache = await redis_conn.hgetall(
            aave_asset_contract_data.format(asset_address),
        )

        if asset_data_cache:
            asset_decimals = asset_data_cache[b'asset_decimals'].decode(
                'utf-8',
            )
            asset_symbol = asset_data_cache[b'asset_symbol'].decode(
                'utf-8',
            )
            asset_name = asset_data_cache[b'asset_name'].decode(
                'utf-8',
            )
        else:

            asset_contract_obj = current_node['web3_client'].eth.contract(
                address=Web3.toChecksumAddress(asset_address),
                abi=erc20_abi,
            )

            tasks = [
                asset_contract_obj.functions.decimals(),
                asset_contract_obj.functions.symbol(),
                asset_contract_obj.functions.name(),
            ]

            [
                asset_decimals,
                asset_symbol,
                asset_name,
            ] = await rpc_helper.web3_call(tasks, redis_conn=redis_conn)

            await redis_conn.hset(
                name=aave_asset_contract_data.format(asset_address),
                mapping={
                    'asset_decimals': asset_decimals,
                    'asset_symbol': asset_symbol,
                    'asset_name': asset_name,
                },
            )

        return {
            'address': asset_address,
            'decimals': asset_decimals,
            'symbol': asset_symbol,
            'name': asset_name,
        }

    except Exception as err:
        # this will be retried in next cycle
        helper_logger.opt(exception=True).error(
            (
                f'RPC error while fetcing metadata for asset {asset_address},'
                f' error_msg:{err}'
            ),
        )
        raise err


async def get_supply_events(
    rpc_helper: RpcHelper,
    from_block: int,
    to_block: int,
    redis_conn: aioredis.Redis,

):
    event_sig, event_abi = get_event_sig_and_abi(
        {
            'Supply': AAVE_EVENT_SIGS['Supply'],
            'Withdraw': AAVE_EVENT_SIGS['Withdraw'],
            'LiquidationCall': AAVE_EVENT_SIGS['LiquidationCall'],
            'ReserveDataUpdated': AAVE_EVENT_SIGS['ReserveDataUpdated'],
            'Borrow': AAVE_EVENT_SIGS['Borrow'],

        },
        AAVE_EVENTS_ABI,
    )

    events = await rpc_helper.get_events_logs(
        contract_address=worker_settings.contract_addresses.aave_v3_pool,
        to_block=to_block,
        from_block=from_block,
        topics=[event_sig],
        event_abi=event_abi,
        redis_conn=redis_conn,
    )

    return events
