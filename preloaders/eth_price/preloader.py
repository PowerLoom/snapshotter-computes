from redis import asyncio as aioredis
import json
import asyncio

from snapshotter.utils.callback_helpers import GenericPreloader
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import EpochBase
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.file_utils import read_json_file
from snapshotter.utils.rpc import get_contract_abi_dict
from computes.redis_keys import uniswap_eth_usd_price_zset
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
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

        self.dai_weth_pair = worker_settings.contract_addresses.DAI_WETH_PAIR
        self.usdc_weth_pair = worker_settings.contract_addresses.USDC_WETH_PAIR
        self.usdbc_weth_pair = worker_settings.contract_addresses.USDbC_WETH_PAIR
        # Token decimals for price calculations
        self.TOKENS_DECIMALS = {
            'USDbC': 6,
            'DAI': 18,
            'USDC': 6,
            'WETH': 18,
        }
        # Load pair contract ABI
        self.pair_contract_abi = read_json_file(
            worker_settings.uniswap_contract_abis.pair_contract_v3,
            self._logger,
        )

    @staticmethod
    def sqrtPriceX96ToTokenPricesNoDecimals(sqrtPriceX96):
        price0 = ((sqrtPriceX96 / (2**96))** 2)
        price1 = 1 / price0
        return price0, price1

    @staticmethod
    def sqrtPriceX96ToTokenPrices(sqrtPriceX96, token0_decimals, token1_decimals):
        # https://blog.uniswap.org/uniswap-v3-math-primer

        price0 = ((sqrtPriceX96 / (2**96))** 2) / (10 ** token1_decimals / 10 ** token0_decimals)
        price1 = 1 / price0

        price0 = round(price0, token0_decimals)
        price1 = round(price1, token1_decimals)

        return price0, price1

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
            dai_eth_slot0_list = await rpc_helper.batch_eth_call_on_block_range(
                abi_dict=pair_abi_dict,
                function_name='slot0',
                contract_address=self.dai_weth_pair,
                from_block=from_block,
                to_block=to_block,
            )
            usdc_eth_slot0_list = await rpc_helper.batch_eth_call_on_block_range(
                abi_dict=pair_abi_dict,
                function_name='slot0',
                contract_address=self.usdc_weth_pair,
                from_block=from_block,
                to_block=to_block,
            )
            usdbc_eth_slot0_list = await rpc_helper.batch_eth_call_on_block_range(
                abi_dict=pair_abi_dict,
                function_name='slot0',
                contract_address=self.usdbc_weth_pair,
                from_block=from_block,
                to_block=to_block,
            )

            # Calculate ETH price for each block using a weighted average of the three pairs
            for block_count, block_num in enumerate(range(from_block, to_block + 1), start=0):
                dai_eth_sqrt_price_x96 = dai_eth_slot0_list[block_count][0]
                usdc_eth_sqrt_price_x96 = usdc_eth_slot0_list[block_count][0]
                usdbc_eth_sqrt_price_x96 = usdbc_eth_slot0_list[block_count][0]

                dai_eth_price, _ = self.sqrtPriceX96ToTokenPrices(
                    sqrtPriceX96=dai_eth_sqrt_price_x96,
                    token0_decimals=self.TOKENS_DECIMALS['WETH'],
                    token1_decimals=self.TOKENS_DECIMALS['DAI'],
                )

                usdc_eth_price, _ = self.sqrtPriceX96ToTokenPrices(
                    sqrtPriceX96=usdc_eth_sqrt_price_x96,
                    token0_decimals=self.TOKENS_DECIMALS['WETH'],
                    token1_decimals=self.TOKENS_DECIMALS['USDC'],
                )

                usdbc_eth_price, _ = self.sqrtPriceX96ToTokenPrices(
                    sqrtPriceX96=usdbc_eth_sqrt_price_x96,
                    token0_decimals=self.TOKENS_DECIMALS['WETH'],
                    token1_decimals=self.TOKENS_DECIMALS['USDbC'],
                )

                # using fixed weightage for now, will use liquidity based weightage later
                eth_price_usd = (dai_eth_price + usdc_eth_price + usdbc_eth_price) / 3
                eth_price_usd_dict[block_num] = float(eth_price_usd)

                redis_cache_mapping[
                    json.dumps(
                        {'blockHeight': block_num, 'price': float(eth_price_usd)},
                    )
                ] = int(block_num)

            # cache price at height
            source_chain_epoch_size = int(
                await redis_conn.get(source_chain_epoch_size_key()),
            )

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