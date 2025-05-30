from redis import asyncio as aioredis
import json
import asyncio

from rpc_helper.rpc import RpcHelper
from rpc_helper.rpc import get_contract_abi_dict

from snapshotter.utils.callback_helpers import GenericPreloader
from snapshotter.utils.data_utils import get_source_chain_block_time
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import EpochBase
from snapshotter.utils.file_utils import read_json_file
from computes.redis_keys import uniswap_eth_usd_price_zset
from snapshotter.utils.redis.redis_keys import source_chain_block_time_key
from computes.settings.config import settings as worker_settings

SECONDS_IN_7_DAYS = 7 * 24 * 60 * 60


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
        self.usdc_weth_pair = '0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640'
        # Token decimals for price calculations
        self.TOKENS_DECIMALS = {
            'USDC': 6,
            'WETH': 18,
        }
        # Load pair contract ABI
        self.pair_contract_abi = read_json_file(
            worker_settings.uniswap_contract_abis.pair_contract,
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
            usdc_eth_slot0_list = await rpc_helper.batch_eth_call_on_block_range(
                abi_dict=pair_abi_dict,
                function_name='slot0',
                contract_address=self.usdc_weth_pair,
                from_block=from_block,
                to_block=to_block,
            )

            for block_count, block_num in enumerate(range(from_block, to_block + 1), start=0):
                usdc_eth_sqrt_price_x96 = usdc_eth_slot0_list[block_count][0]

                _, eth_price_usd = self.sqrtPriceX96ToTokenPrices(
                    sqrtPriceX96=usdc_eth_sqrt_price_x96,
                    token0_decimals=self.TOKENS_DECIMALS['USDC'],
                    token1_decimals=self.TOKENS_DECIMALS['WETH'],
                )
                # using fixed weightage for now, will use liquidity based weightage later
                eth_price_usd_dict[block_num] = float(eth_price_usd)

                redis_cache_mapping[
                    json.dumps(
                        {'blockHeight': block_num, 'price': float(eth_price_usd)},
                    )
                ] = int(block_num)

            source_chain_block_time = await redis_conn.get(source_chain_block_time_key())
            if source_chain_block_time and source_chain_block_time > 0:
                num_blocks_in_7_days = SECONDS_IN_7_DAYS / source_chain_block_time
                pruning_max_score = int(from_block) - int(num_blocks_in_7_days)
            else:
                self._logger.warning("Source chain block time not found in Redis. Using default pruning logic.")
                pruning_max_score = int(from_block) - 50400

            await asyncio.gather(
                redis_conn.zadd(
                    name=uniswap_eth_usd_price_zset,
                    mapping=redis_cache_mapping,
                ),
                redis_conn.zremrangebyscore(
                    name=uniswap_eth_usd_price_zset,
                    min=0,
                    max=pruning_max_score,
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