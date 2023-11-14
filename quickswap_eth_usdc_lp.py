import json
from typing import List
from typing import Tuple
from typing import Union

from redis import asyncio as aioredis

from .utils.event_log_decoder import EventLogDecoder
from .utils.models.message_models import QuickswapLPDepositSnapshot
from .utils.models.message_models import QuickswapLPIncreaseLiquiditySnapshot
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import EthTransactionReceipt
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.redis.redis_keys import epoch_txs_htable
from snapshotter.utils.rpc import RpcHelper


class QuickswapEthUSDCLPProcessor(GenericProcessorSnapshot):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='QuickswapEthUSDCLPProcessor')

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,

    ) -> Union[None, List[Union[Tuple[str, QuickswapLPDepositSnapshot], Tuple[str, QuickswapLPIncreaseLiquiditySnapshot]]]]:
        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        if max_chain_height != min_chain_height:
            self._logger.error('Currently only supports single block height')
            raise Exception('Currently only supports single block height')

        # get txs for this epoch
        txs_hset = await redis_conn.hgetall(epoch_txs_htable(epoch.epochId))
        all_txs = {k.decode(): EthTransactionReceipt.parse_raw(v) for k, v in txs_hset.items()}

        contract_address = '0x8480199E5D711399ABB4D51bDa329E064c89ad77'
        NFT_POSITION_MANAGER_ADDRESS = '0xd8E1E7009802c914b0d39B31Fc1759A865b727B1'
        LP_PAIR_ADDRESS = '0x04c6b11E1Ffe1F1032BD62adb343C9D07767489c'

        contract_txs = list(
            map(
                lambda x: x.dict(), filter(
                    lambda tx: tx.to == contract_address or tx.to == NFT_POSITION_MANAGER_ADDRESS,
                    all_txs.values(),
                ),
            ),
        )

        with open('snapshotter/modules/boost/static/abis/v3_lp_pair.json') as f:
            abi = json.load(f)

        node = rpc_helper.get_current_node()
        w3 = node['web3_client']
        contract = w3.eth.contract(address=contract_address, abi=abi)

        eld = EventLogDecoder(contract)

        snapshots = []

        for tx_receipt in contract_txs:
            for log in tx_receipt['logs']:
                # Deposit (index_topic_1 address sender, index_topic_2 address to, uint256 shares, uint256 amount0, uint256 amount1) event topic
                if log['topics'][0] == '0x4e2ca0515ed1aef1395f66b5303bb5d6f1bf9d61a353fa53f73f8ac9973fa9f6' and log['address'] == LP_PAIR_ADDRESS:
                    try:
                        decoded_log = eld.decode_log(log)
                        snapshots.append(
                            (
                                decoded_log['to'],
                                QuickswapLPDepositSnapshot(
                                    address=decoded_log['to'],
                                    sender=decoded_log['sender'],
                                    shares=decoded_log['shares'],
                                    amount0=decoded_log['amount0'],
                                    amount1=decoded_log['amount1'],
                                ),
                            ),
                        )
                    except:
                        continue

                # IncreaseLiquidity (index_topic_1 uint256 tokenId, uint128 liquidity, uint128 actualLiquidity, uint256 amount0, uint256 amount1, address pool)
                if log['topics'][0] == '0x8a82de7fe9b33e0e6bca0e26f5bd14a74f1164ffe236d50e0a36c3ea70f2b814' and log['address'] == NFT_POSITION_MANAGER_ADDRESS:
                    try:
                        decoded_log = eld.decode_log(log)
                        if decoded_log['pool'] == '0xc44ad482f24fd750caeba387d2726d8653f8c4bb':
                            snapshots.append(
                                (
                                    tx_receipt['from_field'],
                                    QuickswapLPIncreaseLiquiditySnapshot(
                                        tokenId=decoded_log['tokenId'],
                                        liquidity=decoded_log['liquidity'],
                                        actualLiquidity=decoded_log['actualLiquidity'],
                                        amount0=decoded_log['amount0'],
                                        amount1=decoded_log['amount1'],
                                        pool=decoded_log['pool'],
                                    ),
                                ),
                            )
                    except Exception as e:
                        continue

        return snapshots
