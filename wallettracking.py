import json
from typing import List, Tuple, Union

from redis import asyncio as aioredis

from .utils.models.message_models import WalletTrackerSnapShot
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import EthTransactionReceipt
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.redis.redis_keys import epoch_txs_htable
from snapshotter.utils.rpc import RpcHelper


class WalletTrackerProcessor(GenericProcessorSnapshot):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='WalletTrackerProcessor')

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
    ) -> Union[None, List[Tuple[str, WalletTrackerSnapShot]]]:
        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        if max_chain_height != min_chain_height:
            self._logger.error('Currently only supports single block height')
            raise Exception('Currently only supports single block height')

        # get txs for this epoch
        txs_hset = await redis_conn.hgetall(epoch_txs_htable(epoch.epochId))
        all_txs = {
            k.decode(): EthTransactionReceipt.parse_raw(v)
            for k, v in txs_hset.items()
        }

        wallet_addresses = ['0x95222290DD7278Aa3Ddd389Cc1E1d165CC4BAfe5']
        snapshots = []

        for wallet_address in wallet_addresses:
            wallet_txs = list(
                map(
                    lambda x: x.dict(),
                    filter(
                        lambda tx: tx.from_field == wallet_address and tx.to,
                        all_txs.values(),
                    ),
                ),
            )

            

            for tx in wallet_txs:
                snapshots.append(
                    (
                        f"{wallet_address}_{tx['to']}",
                        WalletTrackerSnapShot(
                            wallet_address=wallet_address,
                            transactionHash=tx['transactionHash'],
                            contract_address=tx['to'],
                            timestamp_test=tx['timestamp'],
                            gasUsed_test=tx['gasUsed'],
                        ),
                    ),
                )

        return snapshots
