import asyncio
import math

from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import sqrtPriceX96ToTokenPrices
from web3 import Web3

from snapshotter.modules.computes.settings.config import settings as worker_settings
from snapshotter.modules.computes.utils.constants import current_node
from snapshotter.modules.computes.utils.constants import erc20_abi
from snapshotter.modules.computes.utils.constants import factory_contract_obj
from snapshotter.modules.computes.utils.constants import pair_contract_abi
from snapshotter.modules.computes.utils.constants import STABLE_TOKENS_LIST
from snapshotter.modules.computes.utils.constants import TOKENS_DECIMALS
from snapshotter.modules.computes.utils.constants import ZER0_ADDRESS

helper_logger = logger.bind(module='PowerLoom|Uniswap|Helpers')


def get_maker_pair_data(prop):
    """
    Get Maker token data based on the given property.
    
    Args:
        prop (str): The property to retrieve ('name', 'symbol', or other).
    
    Returns:
        str: The requested Maker token data.
    """
    prop = prop.lower()
    if prop == 'name':
        return 'Maker'
    elif prop == 'symbol':
        return 'MKR'
    else:
        return 'Maker'


async def get_pair(
    factory_contract_obj,
    token0,
    token1,
    fee,
    rpc_helper: RpcHelper,
):
    """
    Get the pair address for two tokens and the given fee.

    Args:
        factory_contract_obj: The factory contract object.
        token0 (str): The address of the first token.
        token1 (str): The address of the second token.
        fee (int): The fee for the pair.
        rpc_helper (RpcHelper): Helper for making RPC calls.

    Returns:
        str: The pair address.
    """
    tasks = [
        factory_contract_obj.functions.getPool(
            Web3.to_checksum_address(token0),
            Web3.to_checksum_address(token1),
            fee,
        ),
    ]


    result = await rpc_helper.web3_call(
        tasks=tasks,
    )
    pair = result[0]

    return pair


async def get_pair_metadata(
    pair_address,
    rpc_helper: RpcHelper,
):
    """
    Get information on the tokens contained within a pair contract.

    This function retrieves the name, symbol, and decimals of token0 and token1,
    as well as the pair symbol by concatenating {token0Symbol}-{token1Symbol}.

    Args:
        pair_address (str): The address of the pair contract.
        rpc_helper (RpcHelper): Helper for making RPC calls.

    Returns:
        dict: A dictionary containing token and pair information.

    Raises:
        Exception: If there's an error fetching metadata for the pair.
    """
    try:
        pair_address = Web3.to_checksum_address(pair_address)
        pair_contract = current_node['web3_client'].eth.contract(
            address=pair_address,
            abi=pair_contract_abi,
        )

        tasks = [
            pair_contract.functions.token0(),
            pair_contract.functions.token1(),
            pair_contract.functions.fee(),
        ]

        token0Addr, token1Addr, fee = await rpc_helper.web3_call(
            tasks=tasks,
        )

        # token0 contract
        token0 = current_node['web3_client'].eth.contract(
            address=Web3.to_checksum_address(token0Addr),
            abi=erc20_abi,
        )
        # token1 contract
        token1 = current_node['web3_client'].eth.contract(
            address=Web3.to_checksum_address(token1Addr),
            abi=erc20_abi,
        )

        # prepare tasks for fetching token data
        token0_tasks = []
        token1_tasks = []

        # special case to handle maker token
        maker_token0 = None
        maker_token1 = None

        if Web3.to_checksum_address(token0Addr) == Web3.to_checksum_address(worker_settings.contract_addresses.MAKER):
            token0_name = get_maker_pair_data('name')
            token0_symbol = get_maker_pair_data('symbol')
            maker_token0 = True
        else:
            token0_tasks.extend([token0.functions.name(), token0.functions.symbol()])
        token0_tasks.append(token0.functions.decimals())

        if Web3.to_checksum_address(token1Addr) == Web3.to_checksum_address(worker_settings.contract_addresses.MAKER):
            token1_name = get_maker_pair_data('name')
            token1_symbol = get_maker_pair_data('symbol')
            maker_token1 = True
        else:
            token1_tasks.extend([token1.functions.name(), token1.functions.symbol()])
        token1_tasks.append(token1.functions.decimals())

        if maker_token1:
            [token0_name, token0_symbol, token0_decimals] = await rpc_helper.web3_call(
                tasks=token0_tasks,
            )
            [token1_decimals] = await rpc_helper.web3_call(
                token1_tasks,
            )
        elif maker_token0:
            [token1_name, token1_symbol, token1_decimals] = await rpc_helper.web3_call(
                tasks=token1_tasks,
            )
            [token0_decimals] = await rpc_helper.web3_call(
                tasks=token0_tasks,
            )
        else:
            [token0_name, token0_symbol, token0_decimals] = await rpc_helper.web3_call(
                tasks=token0_tasks,
            )
            [token1_name, token1_symbol, token1_decimals] = await rpc_helper.web3_call(
                tasks=token1_tasks,
            )

        return {
            'token0': {
                'address': token0Addr,
                'name': token0_name,
                'symbol': token0_symbol,
                'decimals': token0_decimals,
            },
            'token1': {
                'address': token1Addr,
                'name': token1_name,
                'symbol': token1_symbol,
                'decimals': token1_decimals,
            },
            'pair': {
                'symbol': f'{token0_symbol}-{token1_symbol}|{fee}',
                'address': pair_address,
                'fee': fee,
            },
        }
    except Exception as err:
        # this will be retried in next cycle
        helper_logger.opt(exception=True).error(
            (
                f'RPC error while fetcing metadata for pair {pair_address},'
                f' error_msg:{err}'
            ),
        )
        raise err


async def get_token_eth_price_dict(
    token_address: str,
    token_decimals: int,
    from_block,
    to_block,
    rpc_helper: RpcHelper,
    eth_price_dict: dict,
):
    """
    Get a dictionary of token prices in ETH for each block.

    Args:
        token_address (str): The address of the token.
        token_decimals (int): The number of decimals for the token.
        from_block (int): The starting block number.
        to_block (int): The ending block number.
        rpc_helper (RpcHelper): Helper for making RPC calls.

    Returns:
        dict: A dictionary mapping block numbers to token prices in ETH.

    Raises:
        Exception: If there's an error fetching token prices.
    """
    token_address = Web3.to_checksum_address(token_address)
    token_eth_price_dict = dict()

    try:
        token_eth_quote = await get_token_eth_quote_from_uniswap(
            token_address=token_address,
            token_decimals=token_decimals,
            from_block=from_block,
            to_block=to_block,
            rpc_helper=rpc_helper,
            eth_price_dict=eth_price_dict,
        )

        if token_eth_quote:
            token_eth_quote = [quote[0] for quote in token_eth_quote]
            for block_num, price in zip(range(from_block, to_block + 1), token_eth_quote):
                token_eth_price_dict[block_num] = price

        return token_eth_price_dict

    except Exception as e:
        helper_logger.debug(f'error while fetching token price for {token_address}, error_msg:{e}')
        raise e


async def get_token_pair_address_with_fees(
    token0: str,
    token1: str,
    rpc_helper: RpcHelper,
):
    """
    Get the best pair address for two tokens based on liquidity across different fee tiers.

    Args:
        token0 (str): The address of the first token.
        token1 (str): The address of the second token.
        rpc_helper (RpcHelper): Helper for making RPC calls.

    Returns:
        str: The address of the best pair contract.
    """
    tasks = [
        get_pair(
            factory_contract_obj=factory_contract_obj, token0=token0, token1=token1,
            fee=int(10000), rpc_helper=rpc_helper,
        ),
        get_pair(
            factory_contract_obj=factory_contract_obj, token0=token0, token1=token1,
            fee=int(3000), rpc_helper=rpc_helper,
        ),
        get_pair(
            factory_contract_obj=factory_contract_obj, token0=token0, token1=token1,
            fee=int(500), rpc_helper=rpc_helper,
        ),
        get_pair(
            factory_contract_obj=factory_contract_obj, token0=token0, token1=token1,
            fee=int(100), rpc_helper=rpc_helper,
        ),
    ]
    pair_address_list = await asyncio.gather(*tasks)
    pair_address_list = [pair for pair in pair_address_list if pair != ZER0_ADDRESS]

    if len(pair_address_list) > 0:
        pair_contracts = [
            current_node['web3_client'].eth.contract(
                address=Web3.to_checksum_address(pair),
                abi=pair_contract_abi,
            ) for pair in pair_address_list
        ]

        tasks = [
            asyncio.create_task(
                rpc_helper.web3_call(
                    tasks=[pair_contract.functions.liquidity()],
                )
            ) for pair_contract in pair_contracts
        ]

        liquidity_list = await asyncio.gather(*tasks)

        pair_liquidity_dict = dict(zip(pair_address_list, liquidity_list))
        best_pair = max(pair_liquidity_dict, key=pair_liquidity_dict.get)
    else:
        best_pair = ZER0_ADDRESS

    return best_pair


async def get_token_stable_pair_data(
    token: str,
    token_decimals: int,
    rpc_helper: RpcHelper,
):
    """
    Get the stable pair data for a given token.

    This function attempts to find a pair between the given token and a stable token.

    Args:
        token (str): The address of the token.
        token_decimals (int): The number of decimals for the token.
        rpc_helper (RpcHelper): Helper for making RPC calls.

    Returns:
        dict: A dictionary containing token pair data.
    """
    token_stable_pair = ZER0_ADDRESS
    token0 = token
    token1 = ZER0_ADDRESS
    token0_decimals = token_decimals
    token1_decimals = 0
    for stable_token in STABLE_TOKENS_LIST:
        if int(token, 16) < int(stable_token, 16):
            token0, token1 = token, stable_token
            token0_decimals, token1_decimals = token_decimals, TOKENS_DECIMALS.get(stable_token, 0)
        else:
            token0, token1 = stable_token, token
            token0_decimals, token1_decimals = TOKENS_DECIMALS.get(stable_token, 0), token_decimals

        pair = await get_token_pair_address_with_fees(
            token0=token0,
            token1=token1,
            rpc_helper=rpc_helper,
        )

        if pair != ZER0_ADDRESS:
            token_stable_pair = pair
            break

    return {
        'token0': token0,
        'token1': token1,
        'token0_decimals': int(token0_decimals),
        'token1_decimals': int(token1_decimals),
        'pair': token_stable_pair,
    }


async def get_token_eth_quote_from_uniswap(
    token_address,
    token_decimals,
    from_block,
    to_block,
    rpc_helper: RpcHelper,
    eth_price_dict: dict,
):
    """
    Get the ETH quote for a token from Uniswap.

    This function first attempts to price from a token-WETH pool. If that fails,
    it tries to find a token-stable coin pool and calculates the ETH price.

    Args:
        token_address (str): The address of the token.
        token_decimals (int): The number of decimals for the token.
        from_block (int): The starting block number.
        to_block (int): The ending block number.
        rpc_helper (RpcHelper): Helper for making RPC calls.

    Returns:
        list: A list of tuples containing token prices in ETH for each block.

    Raises:
        Exception: If there's an error fetching token prices.
    """
    token0 = token_address
    token1 = worker_settings.contract_addresses.WETH
    token0_decimals = token_decimals
    token1_decimals = TOKENS_DECIMALS.get(worker_settings.contract_addresses.WETH, 18)
    if int(token1, 16) < int(token0, 16):
        token0, token1 = token1, token0
        token0_decimals, token1_decimals = token1_decimals, token0_decimals

    try:
        token_weth_pair = await get_token_pair_address_with_fees(
            token0=token0,
            token1=token1,
            rpc_helper=rpc_helper,
        )

        token_eth_quote = []

        if token_weth_pair != ZER0_ADDRESS:
            response = await rpc_helper.batch_eth_call_on_block_range(
                abi_dict=get_contract_abi_dict(
                    abi=pair_contract_abi,
                ),
                contract_address=token_weth_pair,
                from_block=from_block,
                to_block=to_block,
                function_name='slot0',
                params=[],
            )
            sqrtP_list = [slot0[0] for slot0 in response]
            for sqrtP in sqrtP_list:
                price0, price1 = sqrtPriceX96ToTokenPrices(
                    sqrtP,
                    token0_decimals,
                    token1_decimals,
                )

                if token0.lower() == token_address.lower():
                    token_eth_quote.append((price0,))
                else:
                    token_eth_quote.append((price1,))

            return token_eth_quote
        else:
            # since we couldnt find a token/weth pool, attempt to find a token/stable pool
            token_stable_pair_data = await get_token_stable_pair_data(
                token=token_address,
                token_decimals=token_decimals,
                rpc_helper=rpc_helper,
            )

            if token_stable_pair_data['pair'] != ZER0_ADDRESS:
                response = await rpc_helper.batch_eth_call_on_block_range(
                    abi_dict=get_contract_abi_dict(
                        abi=pair_contract_abi,
                    ),
                    contract_address=token_stable_pair_data['pair'],
                    from_block=from_block,
                    to_block=to_block,
                    function_name='slot0',
                    params=[],
                )

                eth_usd_price_dict = eth_price_dict

                token0_decimals = token_stable_pair_data['token0_decimals']
                token1_decimals = token_stable_pair_data['token1_decimals']

                sqrtP_list = [slot0[0] for slot0 in response]
                sqrtP_eth_list = [block_price for block_price in eth_usd_price_dict.values()]
                token_eth_quote = []

                for i in range(len(sqrtP_list)):
                    sqrtP = sqrtP_list[i]
                    eth_price = sqrtP_eth_list[i]
                    price0, price1 = sqrtPriceX96ToTokenPrices(
                        sqrtP,
                        token0_decimals,
                        token1_decimals,
                    )
                    if token0.lower() == token_address.lower():
                        token_eth_quote.append((price0 / eth_price,))
                    else:
                        token_eth_quote.append((price1 / eth_price,))

                return token_eth_quote
            else:
                return [(0,) for _ in range(from_block, to_block + 1)]
    except Exception as e:
        helper_logger.debug(f'error while fetching token price for {token_address}, error_msg:{e}')
        raise e


def truncate(number, decimals=5):
    """
    Truncate a number to a specific number of decimal places.

    Args:
        number (float): The number to truncate.
        decimals (int): The number of decimal places to keep (default: 5).

    Returns:
        float: The truncated number.

    Raises:
        TypeError: If decimals is not an integer.
        ValueError: If decimals is negative.
    """
    if not isinstance(decimals, int):
        raise TypeError('decimal places must be an integer.')
    elif decimals < 0:
        raise ValueError('decimal places has to be 0 or more.')
    elif decimals == 0:
        return math.trunc(number)

    factor = 10.0 ** decimals
    return math.trunc(number * factor) / factor