import json

from web3 import Web3

from computes.settings.config import settings as worker_settings
from computes.utils.constants import pair_contract_abi
from computes.utils.constants import TOKENS_DECIMALS
from snapshotter.utils.callback_helpers import GenericPreloader
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.data_models import PreloaderResult
from snapshotter.utils.models.message_models import EpochBase
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.rpc import get_contract_abi_dict


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
        self._logger = logger.bind(module='EthPricePreloader')
        self._dai_weth_pair = Web3.to_checksum_address(worker_settings.contract_addresses.DAI_WETH_PAIR)
        self._usdc_weth_pair = Web3.to_checksum_address(worker_settings.contract_addresses.USDC_WETH_PAIR)
        self._usdt_weth_pair = Web3.to_checksum_address(worker_settings.contract_addresses.USDT_WETH_PAIR)


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

            pair_abi_dict = get_contract_abi_dict(pair_contract_abi)

            # Fetch reserves for each pair across the block range
            dai_eth_slot0_list = await rpc_helper.batch_eth_call_on_block_range(
                abi_dict=pair_abi_dict,
                function_name='slot0',
                contract_address=self._dai_weth_pair,
                from_block=from_block,
                to_block=to_block,
            )
            usdc_eth_slot0_list = await rpc_helper.batch_eth_call_on_block_range(
                abi_dict=pair_abi_dict,
                function_name='slot0',
                contract_address=self._usdc_weth_pair,
                from_block=from_block,
                to_block=to_block,
            )
            usdt_eth_slot0_list = await rpc_helper.batch_eth_call_on_block_range(
                abi_dict=pair_abi_dict,
                function_name='slot0',
                contract_address=self._usdt_weth_pair,
                from_block=from_block,
                to_block=to_block,
            )

            # Calculate ETH price for each block using a weighted average of the three pairs
            for block_count, block_num in enumerate(range(from_block, to_block + 1), start=0):
                dai_eth_sqrt_price_x96 = dai_eth_slot0_list[block_count][0]
                usdc_eth_sqrt_price_x96 = usdc_eth_slot0_list[block_count][0]
                usdt_eth_sqrt_price_x96 = usdt_eth_slot0_list[block_count][0]

                _, dai_eth_price = self.sqrtPriceX96ToTokenPrices(
                    sqrtPriceX96=dai_eth_sqrt_price_x96,
                    token0_decimals=TOKENS_DECIMALS[worker_settings.contract_addresses.DAI],
                    token1_decimals=TOKENS_DECIMALS[worker_settings.contract_addresses.WETH],
                )
                _, usdc_eth_price = self.sqrtPriceX96ToTokenPrices(
                    sqrtPriceX96=usdc_eth_sqrt_price_x96,
                    token0_decimals=TOKENS_DECIMALS[worker_settings.contract_addresses.USDC],
                    token1_decimals=TOKENS_DECIMALS[worker_settings.contract_addresses.WETH],
                )
                usdt_eth_price, _ = self.sqrtPriceX96ToTokenPrices(
                    sqrtPriceX96=usdt_eth_sqrt_price_x96,
                    token0_decimals=TOKENS_DECIMALS[worker_settings.contract_addresses.WETH],
                    token1_decimals=TOKENS_DECIMALS[worker_settings.contract_addresses.USDT],
                )

                # using fixed weightage for now, will use liquidity based weightage later
                eth_price_usd = (dai_eth_price + usdc_eth_price + usdt_eth_price) / 3
                eth_price_usd_dict[block_num] = float(eth_price_usd)

                redis_cache_mapping[
                    json.dumps(
                        {'blockHeight': block_num, 'price': float(eth_price_usd)},
                    )
                ] = int(block_num)

            return eth_price_usd_dict

        except Exception as err:
            self._logger.error(
                f'RPC ERROR failed to fetch ETH price, error_msg:{err}',
            )
            raise err
        
    async def compute(
            self,
            epoch: EpochBase,
            rpc_helper: RpcHelper,
    ) -> PreloaderResult:
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
            eth_price_usd_dict = await self.get_eth_price_usd(
                from_block=min_chain_height,
                to_block=max_chain_height,
                rpc_helper=rpc_helper,
            )
            return PreloaderResult(
                keyword='eth_price',
                result=eth_price_usd_dict,
            )
        except Exception as e:
            # Log any errors that occur during price fetching
            self._logger.error(f'Error in Eth Price preloader: {e}')
            raise e
        
    async def cleanup(self):
        """
        Perform any necessary cleanup operations.

        This method is currently a placeholder and does not perform any actions.
        It can be implemented in the future if cleanup operations are needed.
        """
        pass


eth_price_preloader = EthPricePreloader()

