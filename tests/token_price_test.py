import asyncio
import json
from functools import partial

from redis import asyncio as aioredis
from web3 import Web3

from ..settings.config import enabled_projects
from ..settings.config import settings
from ..settings.config import settings as worker_settings
from ..utils.helpers import get_pair_metadata
from snapshotter.utils.redis.rate_limiter import load_rate_limiter_scripts
from snapshotter.utils.redis.redis_conn import provide_async_redis_conn_insta

# Initialize Web3 instance
w3 = Web3(Web3.HTTPProvider(settings.rpc.full_nodes[0].url))
pair_address = Web3.to_checksum_address(
    '0x97c4adc5d28a86f9470c70dd91dc6cc2f20d2d4d',
)


def read_json_file(file_path: str):
    """
    Read given json file and return its content as a dictionary.

    Args:
        file_path (str): Path to the JSON file.

    Returns:
        dict: Content of the JSON file.

    Raises:
        Exception: If unable to open or read the file.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
    except Exception as exc:
        print(f'Unable to open the {file_path} file')
        raise exc
    return json_data


# Load contract ABIs
router_contract_abi = read_json_file(
    worker_settings.uniswap_contract_abis.router,
)
pair_contract_abi = read_json_file(
    worker_settings.uniswap_contract_abis.pair_contract,
)
all_contracts = enabled_projects


async def get_token_price_at_block_height(
    token_contract_obj,
    token_metadata,
    block_height,
    loop: asyncio.AbstractEventLoop,
    redis_conn=None,
    debug_log=True,
):
    """
    Get the price of a token at a given block height.

    Args:
        token_contract_obj: The token contract object.
        token_metadata (dict): Metadata of the token.
        block_height: The block height to query.
        loop (asyncio.AbstractEventLoop): The event loop.
        redis_conn: Redis connection object (optional).
        debug_log (bool): Whether to print debug logs.

    Returns:
        float: The price of the token.
    """
    try:
        token_price = 0

        # Define stable coins addresses and decimals
        stable_coins_addresses = {
            'USDC': Web3.to_checksum_address(
                worker_settings.contract_addresses.USDC,
            ),
            'DAI': Web3.to_checksum_address(
                worker_settings.contract_addresses.DAI,
            ),
            'USDT': Web3.to_checksum_address(
                worker_settings.contract_addresses.USDT,
            ),
        }
        stable_coins_decimals = {
            'USDT': 6,
            'DAI': 18,
            'USDC': 6,
        }

        # Define non-stable coins addresses and their trading pairs
        non_stable_coins_addresses = {
            Web3.to_checksum_address(worker_settings.contract_addresses.agEUR): {
                'token0': Web3.to_checksum_address(
                    worker_settings.contract_addresses.agEUR,
                ),
                'token1': Web3.to_checksum_address(
                    worker_settings.contract_addresses.FEI,
                ),
                'decimals': 18,
            },
            Web3.to_checksum_address(worker_settings.contract_addresses.SYN): {
                'token0': Web3.to_checksum_address(
                    worker_settings.contract_addresses.SYN,
                ),
                'token1': Web3.to_checksum_address(
                    worker_settings.contract_addresses.FRAX,
                ),
                'decimals': 18,
            },
        }

        # Used to avoid INSUFFICIENT_INPUT_AMOUNT error
        token_amount_multiplier = 10**18

        # Check if token is a stable coin
        if Web3.to_checksum_address(token_metadata['address']) in list(
            stable_coins_addresses.values(),
        ):
            token_price = 1
            if debug_log:
                print(
                    (
                        f"## {token_metadata['symbol']}: ignored stablecoin"
                        f" calculation for token0: {token_metadata['symbol']} -"
                        f' WETH - USDT conversion: {token_price}'
                    ),
                )

        # Check if token has no pair with stablecoin and WETH
        elif non_stable_coins_addresses.get(
            Web3.to_checksum_address(token_metadata['address']),
        ):
            contract_metadata = non_stable_coins_addresses.get(
                Web3.to_checksum_address(token_metadata['address']),
            )
            if not contract_metadata:
                return None
            price_function_token0 = partial(
                token_contract_obj.functions.getAmountsOut(
                    10 ** int(contract_metadata['decimals']),
                    [
                        contract_metadata['token0'],
                        contract_metadata['token1'],
                        Web3.to_checksum_address(
                            worker_settings.contract_addresses.USDC,
                        ),
                    ],
                ).call,
                block_identifier=block_height,
            )
            temp_token_price = await loop.run_in_executor(
                func=price_function_token0,
                executor=None,
            )
            if temp_token_price:
                # Convert to USDC decimals
                temp_token_price = (
                    temp_token_price[2] / 10 ** stable_coins_decimals['USDC']
                    if temp_token_price[2] != 0
                    else 0
                )
                token_price = max(token_price, temp_token_price)

        # Handle other tokens
        elif Web3.to_checksum_address(
            token_metadata['address'],
        ) != Web3.to_checksum_address(worker_settings.contract_addresses.WETH):
            # Iterate over all stable coins to find price
            for key, value in stable_coins_addresses.items():
                try:
                    price_function_token0 = partial(
                        token_contract_obj.functions.getAmountsOut(
                            10 ** int(token_metadata['decimals']),
                            [
                                Web3.to_checksum_address(
                                    token_metadata['address'],
                                ),
                                value,
                            ],
                        ).call,
                        block_identifier=block_height,
                    )
                    temp_token_price = await loop.run_in_executor(
                        func=price_function_token0,
                        executor=None,
                    )
                    if temp_token_price:
                        # Convert to stable coin decimals
                        temp_token_price = (
                            temp_token_price[1] /
                            10 ** stable_coins_decimals[key]
                            if temp_token_price[1] != 0
                            else 0
                        )

                        print(
                            (
                                f"## {token_metadata['symbol']}->{key}: token"
                                f' price: {temp_token_price}'
                            ),
                        )

                        token_price = max(token_price, temp_token_price)
                except Exception as error:
                    # If reverted, it means token doesn't have a pair with this stablecoin
                    if 'execution reverted' in str(error):
                        temp_token_price = 0
                else:
                    # If there was no exception and price is still 0,
                    # increase token amount in path (token->stablecoin)
                    if temp_token_price == 0:
                        price_function_token0 = partial(
                            token_contract_obj.functions.getAmountsOut(
                                10 ** int(token_metadata['decimals']) *
                                token_amount_multiplier,
                                [
                                    Web3.to_checksum_address(
                                        token_metadata['address'],
                                    ),
                                    value,
                                ],
                            ).call,
                            block_identifier=block_height,
                        )
                        temp_token_price = await loop.run_in_executor(
                            func=price_function_token0,
                            executor=None,
                        )
                        if temp_token_price:
                            # Convert to stable coin decimals
                            temp_token_price = (
                                temp_token_price[1] /
                                10 ** stable_coins_decimals[key]
                                if temp_token_price[1] != 0
                                else 0
                            )
                            temp_token_price = (
                                temp_token_price / token_amount_multiplier
                            )

                            print(
                                (
                                    f"## {token_metadata['symbol']}->{key}:"
                                    ' (increased_input_amount) token price :'
                                    f' {temp_token_price}'
                                ),
                            )

                            token_price = max(token_price, temp_token_price)

            print(
                (
                    f"## {token_metadata['symbol']}: chosen token price after"
                    f' all stable coin conversions: {token_price}'
                ),
            )

            # Check if path conversion by token->weth->usdt gives a higher price
            try:
                price_function_token0 = partial(
                    token_contract_obj.functions.getAmountsOut(
                        10 ** int(token_metadata['decimals']),
                        [
                            Web3.to_checksum_address(token_metadata['address']),
                            Web3.to_checksum_address(
                                worker_settings.contract_addresses.WETH,
                            ),
                            Web3.to_checksum_address(
                                worker_settings.contract_addresses.USDT,
                            ),
                        ],
                    ).call,
                    block_identifier=block_height,
                )
                temp_token_price = await loop.run_in_executor(
                    func=price_function_token0,
                    executor=None,
                )
                if temp_token_price:
                    # Convert to USDT decimals
                    temp_token_price = (
                        temp_token_price[2] /
                        10 ** stable_coins_decimals['USDT']
                        if temp_token_price[2] != 0
                        else 0
                    )
                    print(
                        (
                            f"## {token_metadata['symbol']}: token price after"
                            f' weth->stablecoin: {temp_token_price}'
                        ),
                    )
                    token_price = max(token_price, temp_token_price)
            except Exception:
                # Ignore INSUFFICIENT_INPUT_AMOUNT/execution_reverted errors
                pass

            # If price is still 0, increase token amount in path (token->weth-usdt)
            if token_price == 0:
                price_function_token0 = partial(
                    token_contract_obj.functions.getAmountsOut(
                        10 ** int(
                            token_metadata['decimals'],
                        ) * token_amount_multiplier,
                        [
                            Web3.to_checksum_address(token_metadata['address']),
                            Web3.to_checksum_address(
                                worker_settings.contract_addresses.WETH,
                            ),
                            Web3.to_checksum_address(
                                worker_settings.contract_addresses.USDT,
                            ),
                        ],
                    ).call,
                    block_identifier=block_height,
                )
                temp_token_price = await loop.run_in_executor(
                    func=price_function_token0,
                    executor=None,
                )

                if temp_token_price:
                    # Convert to USDT decimals
                    temp_token_price = (
                        temp_token_price[2] /
                        10 ** stable_coins_decimals['USDT']
                        if temp_token_price[2] != 0
                        else 0
                    )
                    temp_token_price = temp_token_price / token_amount_multiplier
                    print(
                        (
                            f"## {token_metadata['symbol']}: token price after"
                            ' weth->stablecoin (increased_input_amount):'
                            f' {temp_token_price}'
                        ),
                    )
                    token_price = max(token_price, temp_token_price)

            if debug_log:
                print(
                    f"## {token_metadata['symbol']}: final price: {token_price}",
                )

        # If token is WETH, directly check its price against stable coin
        else:
            price_function_token0 = partial(
                token_contract_obj.functions.getAmountsOut(
                    10 ** int(token_metadata['decimals']),
                    [
                        Web3.to_checksum_address(
                            worker_settings.contract_addresses.WETH,
                        ),
                        Web3.to_checksum_address(
                            worker_settings.contract_addresses.USDT,
                        ),
                    ],
                ).call,
                block_identifier=block_height,
            )
            token_price = await loop.run_in_executor(
                func=price_function_token0,
                executor=None,
            )
            token_price = (
                token_price[1] / 10 ** stable_coins_decimals['USDT']
            )  # Convert to USDT decimals
            if debug_log:
                print(
                    f"## {token_metadata['symbol']}: final prices:" f' {token_price}',
                )
    except Exception as err:
        print(
            (
                f'Error: failed to fetch token price | error_msg: {str(err)} |'
                f" contract: {token_metadata['address']}"
            ),
        )
    finally:
        return float(token_price)


async def get_all_pairs_token_price(loop, redis_conn: aioredis.Redis = None):
    """
    Get token prices for all pairs.

    Args:
        loop (asyncio.AbstractEventLoop): The event loop.
        redis_conn (aioredis.Redis, optional): Redis connection object.

    Returns:
        None
    """
    router_contract_obj = w3.eth.contract(
        address=Web3.to_checksum_address(
            worker_settings.contract_addresses.iuniswap_v2_router,
        ),
        abi=router_contract_abi,
    )
    rate_limiting_lua_scripts = await load_rate_limiter_scripts(redis_conn)

    for contract in all_contracts:
        pair_per_token_metadata = await get_pair_metadata(
            rate_limit_lua_script_shas=rate_limiting_lua_scripts,
            pair_address=contract,
            loop=loop,
            redis_conn=redis_conn,
        )
        token0, token1 = await asyncio.gather(
            get_token_price_at_block_height(
                router_contract_obj,
                pair_per_token_metadata['token0'],
                'latest',
                loop,
                redis_conn,
            ),
            get_token_price_at_block_height(
                router_contract_obj,
                pair_per_token_metadata['token1'],
                'latest',
                loop,
                redis_conn,
            ),
        )
        print('\n')
        print(
            {
                pair_per_token_metadata['token0']['symbol']: token0,
                pair_per_token_metadata['token1']['symbol']: token1,
                'contract': contract,
            },
        )
        print('\n')


@provide_async_redis_conn_insta
async def get_pair_tokens_price(pair, loop, redis_conn: aioredis.Redis = None):
    """
    Get token prices for a specific pair.

    Args:
        pair (str): The pair address.
        loop (asyncio.AbstractEventLoop): The event loop.
        redis_conn (aioredis.Redis, optional): Redis connection object.

    Returns:
        None
    """
    router_contract_obj = w3.eth.contract(
        address=Web3.to_checksum_address(
            worker_settings.contract_addresses.iuniswap_v2_router,
        ),
        abi=router_contract_abi,
    )

    pair_address = Web3.to_checksum_address(pair)
    rate_limiting_lua_scripts = await load_rate_limiter_scripts(redis_conn)
    pair_per_token_metadata = await get_pair_metadata(
        rate_limit_lua_script_shas=rate_limiting_lua_scripts,
        pair_address=pair_address,
        loop=loop,
        redis_conn=redis_conn,
    )
    print('\n')
    print('\n')
    token0, token1 = await asyncio.gather(
        get_token_price_at_block_height(
            router_contract_obj,
            pair_per_token_metadata['token0'],
            'latest',
            loop,
            redis_conn,
        ),
        get_token_price_at_block_height(
            router_contract_obj,
            pair_per_token_metadata['token1'],
            'latest',
            loop,
            redis_conn,
        ),
    )
    print('\n')
    print(
        {
            pair_per_token_metadata['token0']['symbol']: token0,
            pair_per_token_metadata['token1']['symbol']: token1,
        },
    )
    print('\n')
    await redis_conn.close()


if __name__ == '__main__':
    pair_address = '0x7b73644935b8e68019ac6356c40661e1bc315860'
    loop = asyncio.get_event_loop()
    data = loop.run_until_complete(
        get_pair_tokens_price(pair_address, loop),
    )
    print(f'\n\n{data}\n')
