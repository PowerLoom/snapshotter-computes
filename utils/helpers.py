import asyncio
import json

from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.rpc import get_event_sig_and_abi
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from ..redis_keys import aave_asset_contract_data
from ..redis_keys import aave_cached_block_height_burn_mint_data
from ..redis_keys import aave_cached_block_height_core_event_data
from ..settings.config import settings as worker_settings
from .constants import AAVE_EVENT_SIGS
from .constants import AAVE_EVENTS_ABI
from .constants import current_node
from .constants import erc20_abi
from .constants import pool_data_provider_contract_obj
from .constants import STABLE_BURN_MINT_EVENT_ABI
from .constants import STABLE_BURN_MINT_EVENT_SIGS
from .constants import VARIABLE_BURN_MINT_EVENT_ABI
from .constants import VARIABLE_BURN_MINT_EVENT_SIGS


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
            reserve_address_dict = json.loads(
                asset_data_cache[b'reserve_addresses'].decode(
                    'utf-8',
                ),
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
                pool_data_provider_contract_obj.functions.getReserveTokensAddresses(asset_address),
            ]

            [
                asset_decimals,
                asset_symbol,
                asset_name,
                reserve_addresses,
            ] = await rpc_helper.web3_call(tasks, redis_conn=redis_conn)

            reserve_address_dict = {
                'a_token': reserve_addresses[0],
                'stable_debt_token': reserve_addresses[1],
                'variable_debt_token': reserve_addresses[2],
            }

            await redis_conn.hset(
                name=aave_asset_contract_data.format(asset_address),
                mapping={
                    'asset_decimals': asset_decimals,
                    'asset_symbol': asset_symbol,
                    'asset_name': asset_name,
                    'reserve_addresses': json.dumps(reserve_address_dict),
                },
            )

        return {
            'address': asset_address,
            'decimals': asset_decimals,
            'symbol': asset_symbol,
            'name': asset_name,
            'reserve_addresses': reserve_address_dict,
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


async def get_pool_data_events(
    rpc_helper: RpcHelper,
    from_block: int,
    to_block: int,
    redis_conn: aioredis.Redis,

):
    try:

        cached_event_dict = await redis_conn.zrangebyscore(
            name=aave_cached_block_height_core_event_data,
            min=int(from_block),
            max=int(to_block),
        )

        if cached_event_dict:
            event_dict = {
                json.loads(
                    event.decode(
                        'utf-8',
                    ),
                )['blockHeight']: json.loads(
                    event.decode('utf-8'),
                )['events']
                for event in cached_event_dict
            }

            return event_dict

        else:
            event_sig, event_abi = get_event_sig_and_abi(
                AAVE_EVENT_SIGS,
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

            event_dict = {}

            for block_num in range(from_block, to_block + 1):
                event_dict[block_num] = list(filter(lambda x: x['blockNumber'] == block_num, events))

            if len(event_dict) > 0:

                redis_cache_mapping = {
                    Web3.to_json({'blockHeight': height, 'events': events}): int(height)
                    for height, events in event_dict.items()
                }

                source_chain_epoch_size = int(await redis_conn.get(source_chain_epoch_size_key()))

                await asyncio.gather(
                    redis_conn.zadd(
                        name=aave_cached_block_height_core_event_data,
                        mapping=redis_cache_mapping,
                    ),
                    redis_conn.zremrangebyscore(
                        name=aave_cached_block_height_core_event_data,
                        min=0,
                        max=from_block - source_chain_epoch_size * 3,
                    ),
                )

            return event_dict

    except Exception as err:
        # this will be retried in next cycle
        helper_logger.opt(exception=True).error(
            (
                f'Error while fetcing Aave supply events in block range {from_block} : {to_block}'
            ),
        )
        raise err


async def get_debt_burn_mint_events(
    asset_address: str,
    asset_metadata: dict,
    rpc_helper: RpcHelper,
    from_block: int,
    to_block: int,
    redis_conn: aioredis.Redis,
):
    try:

        cached_event_dict = await redis_conn.zrangebyscore(
            name=aave_cached_block_height_burn_mint_data.format(asset_address),
            min=int(from_block),
            max=int(to_block),
        )

        if cached_event_dict:
            event_dict = {
                json.loads(
                    event.decode(
                        'utf-8',
                    ),
                )['blockHeight']: json.loads(
                    event.decode('utf-8'),
                )['events']
                for event in cached_event_dict
            }
            return event_dict

        variable_event_sig, variable_event_abi = get_event_sig_and_abi(
            VARIABLE_BURN_MINT_EVENT_SIGS,
            VARIABLE_BURN_MINT_EVENT_ABI,
        )

        variable_events = await rpc_helper.get_events_logs(
            contract_address=asset_metadata['reserve_addresses']['variable_debt_token'],
            to_block=to_block,
            from_block=from_block,
            topics=[variable_event_sig],
            event_abi=variable_event_abi,
            redis_conn=redis_conn,
        )

        event_dict = {}

        for block_num in range(from_block, to_block + 1):
            event_dict[block_num] = list(filter(lambda x: x['blockNumber'] == block_num, variable_events))

        stable_event_sig, stable_event_abi = get_event_sig_and_abi(
            STABLE_BURN_MINT_EVENT_SIGS,
            STABLE_BURN_MINT_EVENT_ABI,
        )

        stable_events = await rpc_helper.get_events_logs(
            contract_address=asset_metadata['reserve_addresses']['stable_debt_token'],
            to_block=to_block,
            from_block=from_block,
            topics=[stable_event_sig],
            event_abi=stable_event_abi,
            redis_conn=redis_conn,
        )

        for event in stable_events:
            event_dict[event['blockNumber']].append(event)

        if len(event_dict) > 0:

            redis_cache_mapping = {
                Web3.to_json({'blockHeight': height, 'events': events}): int(height)
                for height, events in event_dict.items()
            }

            source_chain_epoch_size = int(await redis_conn.get(source_chain_epoch_size_key()))

            await asyncio.gather(
                redis_conn.zadd(
                    name=aave_cached_block_height_burn_mint_data.format(asset_address),
                    mapping=redis_cache_mapping,
                ),
                redis_conn.zremrangebyscore(
                    name=aave_cached_block_height_burn_mint_data.format(asset_address),
                    min=0,
                    max=from_block - source_chain_epoch_size * 3,
                ),
            )

        return event_dict

    except Exception as err:
        # this will be retried in next cycle
        helper_logger.opt(exception=True).error(
            (
                f'Error while fetcing Aave variable mint and burn for {asset_address} '
                f'in block range {from_block} : {to_block}'
            ),
        )
        raise err
