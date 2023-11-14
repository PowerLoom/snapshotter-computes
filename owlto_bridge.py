from typing import List
from typing import Tuple
from typing import Union

from redis import asyncio as aioredis

from .utils.models.message_models import OwltoBridgeSnapshot
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import EthTransactionReceipt
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.redis.redis_keys import epoch_txs_htable
from snapshotter.utils.rpc import RpcHelper


class OwltoBridgeProcessor(GenericProcessorSnapshot):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='OwltoBridgeProcessor')

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,

    ) -> Union[None, List[Tuple[str, OwltoBridgeSnapshot]]]:
        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        if max_chain_height != min_chain_height:
            self._logger.error('Currently only supports single block height')
            raise Exception('Currently only supports single block height')

        # get txs for this epoch
        txs_hset = await redis_conn.hgetall(epoch_txs_htable(epoch.epochId))
        all_txs = {k.decode(): EthTransactionReceipt.parse_raw(v) for k, v in txs_hset.items()}

        contract_address = '0x45A318273749d6eb00f5F6cA3bC7cD3De26D642A'
        contract_txs = list(
            map(
                lambda x: x.dict(), filter(
                    lambda tx: tx.from_field == contract_address,
                    all_txs.values(),
                ),
            ),
        )

        relevant_txs = []

        for tx in contract_txs:
            type_ = None
            try:
                type_ = str(tx['type'])
            except:
                continue

            if type_ and type_ == '0x0':
                relevant_txs.append(tx)

        # decoding all relevant txs
        if not relevant_txs:
            return None

        node = rpc_helper.get_current_node()
        w3 = node['web3_client']

        snapshots = []

        min_amount = 10000000000000000
        for tx_receipt in relevant_txs:
            transaction = w3.eth.getTransaction(tx_receipt['transactionHash'])
            if transaction['value'] > min_amount:

                self._logger.info('Found relevant tx')

                snapshots.append(
                    (
                        transaction.to.lower(),
                        OwltoBridgeSnapshot(
                            receiver=transaction.to,
                            amount=transaction['value'],
                        ),
                    ),
                )
            else:
                self._logger.info('Found tx with amount less than min amount, ignoring!')

        return snapshots
