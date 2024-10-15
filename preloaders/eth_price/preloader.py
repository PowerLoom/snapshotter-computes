from redis import asyncio as aioredis
import json
import asyncio

from snapshotter.utils.callback_helpers import GenericPreloader
from snapshotter.utils.default_logger import logger
from snapshotter.utils.file_utils import read_json_file
from snapshotter.utils.models.message_models import EpochBase
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import RpcHelper
from computes.redis_keys import uniswap_eth_usd_price_zset
from computes.settings.config import settings as worker_settings


class EthPricePreloader(GenericPreloader):
    """
    A preloader class for fetching Ethereum prices for a range of blocks.
    
    This class extends GenericPreloader and implements methods to compute
    and store Ethereum prices for a given epoch range.
    """

    def __init__(self) -> None:
        """
        Initialize the EthPricePreloader with a logger.
        """
        self._logger = logger.bind(module='BlockDetailsPreloader')

        self.dai_weth_pair = '0xa478c2975ab1ea89e8196811f51a7b7ade33eb11'
        self.usdc_weth_pair = '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc'
        self.usdt_weth_pair = '0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852'
        # Token decimals for price calculations
        self.TOKENS_DECIMALS = {
            'USDT': 6,
            'DAI': 18,
            'USDC': 6,
            'WETH': 18,
        }
        # Load pair contract ABI
        self.pair_contract_abi = read_json_file(
            worker_settings.uniswap_contract_abis.pair_contract,
            self._logger,
        )

    def calculate_token_price(self, reserves, token0, token1, reverse=False):
        """
        Calculate the price of token0 in terms of token1 based on the reserves.

        Args:
            reserves (list): List of reserves [reserve0, reserve1, timestamp]
            token0 (str): The first token in the pair
            token1 (str): The second token in the pair
            reverse (bool): If True, calculate price of token1 in terms of token0

        Returns:
            float: The calculated price
        """
        reserve0 = reserves[0] / 10 ** self.TOKENS_DECIMALS[token0]
        reserve1 = reserves[1] / 10 ** self.TOKENS_DECIMALS[token1]
        return reserve1 / reserve0 if reverse else reserve0 / reserve1

    async def get_eth_price_usd(
        self,
        from_block,
        to_block,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
    ):
        """
        Fetches the ETH price in USD for a given block range using Uniswap DAI/ETH, USDC/ETH and USDT/ETH pairs.

        Args:
            from_block (int): The starting block number.
            to_block (int): The ending block number.
            redis_conn (aioredis.Redis): The Redis connection object.
            rpc_helper (RpcHelper): The RPC helper object.

        Returns:
            dict: A dictionary containing the ETH price in USD for each block in the given range.

        Raises:
            Exception: If there's an error fetching the ETH price.
        """
        try:
            eth_price_usd_dict = dict()
            redis_cache_mapping = dict()

            # Check if prices are already cached in Redis
            cached_price_dict = await redis_conn.zrangebyscore(
                name=uniswap_eth_usd_price_zset,
                min=int(from_block),
                max=int(to_block),
            )
            if cached_price_dict and len(cached_price_dict) == to_block - (from_block - 1):
                # If all prices are cached, return them
                price_dict = {
                    json.loads(price.decode('utf-8'))['blockHeight']:
                    json.loads(price.decode('utf-8'))['price']
                    for price in cached_price_dict
                }
                return price_dict

            pair_abi_dict = get_contract_abi_dict(self.pair_contract_abi)

            # Fetch reserves for each pair across the block range
            dai_eth_pair_reserves_list = await rpc_helper.batch_eth_call_on_block_range(
                abi_dict=pair_abi_dict,
                function_name='getReserves',
                contract_address=self.dai_weth_pair,
                from_block=from_block,
                to_block=to_block,
            )
            usdc_eth_pair_reserves_list = await rpc_helper.batch_eth_call_on_block_range(
                abi_dict=pair_abi_dict,
                function_name='getReserves',
                contract_address=self.usdc_weth_pair,
                from_block=from_block,
                to_block=to_block,
            )
            eth_usdt_pair_reserves_list = await rpc_helper.batch_eth_call_on_block_range(
                abi_dict=pair_abi_dict,
                function_name='getReserves',
                contract_address=self.usdt_weth_pair,
                from_block=from_block,
                to_block=to_block,
            )

            # Calculate ETH price for each block
            for block_count, block_num in enumerate(range(from_block, to_block + 1), start=0):
                # Calculate prices for each pair
                dai_price = self.calculate_token_price(dai_eth_pair_reserves_list[block_count], 'DAI', 'WETH')
                usdc_price = self.calculate_token_price(usdc_eth_pair_reserves_list[block_count], 'USDC', 'WETH')
                usdt_price = self.calculate_token_price(eth_usdt_pair_reserves_list[block_count], 'USDT', 'WETH', reverse=True)

                # Calculate total ETH liquidity
                total_eth_liquidity = (
                    dai_eth_pair_reserves_list[block_count][1] / 10 ** self.TOKENS_DECIMALS['WETH'] +
                    usdc_eth_pair_reserves_list[block_count][1] / 10 ** self.TOKENS_DECIMALS['WETH'] +
                    eth_usdt_pair_reserves_list[block_count][0] / 10 ** self.TOKENS_DECIMALS['WETH']
                )

                # Calculate weights for each pair
                daiWeight = (dai_eth_pair_reserves_list[block_count][1] / 10 ** self.TOKENS_DECIMALS['WETH']) / total_eth_liquidity
                usdcWeight = (usdc_eth_pair_reserves_list[block_count][1] / 10 ** self.TOKENS_DECIMALS['WETH']) / total_eth_liquidity
                usdtWeight = (eth_usdt_pair_reserves_list[block_count][0] / 10 ** self.TOKENS_DECIMALS['WETH']) / total_eth_liquidity

                # Calculate weighted average ETH price
                eth_price_usd = (
                    daiWeight * dai_price +
                    usdcWeight * usdc_price +
                    usdtWeight * usdt_price
                )

                eth_price_usd_dict[block_num] = float(eth_price_usd)
                redis_cache_mapping[
                    json.dumps(
                        {'blockHeight': block_num, 'price': float(eth_price_usd)},
                    )
                ] = int(block_num)

            # Cache prices and prune old data
            source_chain_epoch_size = int(await redis_conn.get(source_chain_epoch_size_key()))
            await asyncio.gather(
                redis_conn.zadd(
                    name=uniswap_eth_usd_price_zset,
                    mapping=redis_cache_mapping,
                ),
                redis_conn.zremrangebyscore(
                    name=uniswap_eth_usd_price_zset,
                    min=0,
                    max=int(from_block) - source_chain_epoch_size * 4,
                ),
            )

            return eth_price_usd_dict

        except Exception as err:
            self._logger.error(
                f'RPC ERROR failed to fetch ETH price, error_msg:{err}',
            )
            raise err

    async def compute(
            self,
            epoch: EpochBase,
            redis_conn: aioredis.Redis,
            rpc_helper: RpcHelper,
    ):
        """
        Compute and store Ethereum prices for the given epoch range.

        Args:
            epoch (EpochBase): The epoch containing the block range.
            redis_conn (aioredis.Redis): Redis connection for caching.
            rpc_helper (RpcHelper): Helper for making RPC calls.

        Returns:
            None
        """
        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        try:
            # Fetch Ethereum prices for all blocks in the specified range
            await self.get_eth_price_usd(
                from_block=min_chain_height,
                to_block=max_chain_height,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
            )
        except Exception as e:
            # Log any errors that occur during price fetching
            self._logger.error(f'Error in Eth Price preloader: {e}')
            raise e
        finally:
            # Ensure Redis connection is closed after operation
            await redis_conn.close()

    async def cleanup(self):
        """
        Perform any necessary cleanup operations.

        This method is currently a placeholder and does not perform any actions.
        It can be implemented in the future if cleanup operations are needed.
        """
        pass


eth_price_preloader = EthPricePreloader()