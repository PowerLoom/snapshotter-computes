import json
from typing import List
from typing import Tuple
from typing import Union

from redis import asyncio as aioredis

from .utils.event_log_decoder import EventLogDecoder
from .utils.models.message_models import TrackingWalletInteractionSnapshot
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import EthTransactionReceipt
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.redis.redis_keys import epoch_txs_htable
from snapshotter.utils.rpc import RpcHelper


class TrackingWalletInteractionProcessor(GenericProcessorSnapshot):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='TrackingWalletInteractionProcessor')

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,

    ) -> Union[None, List[Tuple[str, TrackingWalletInteractionSnapshot]]]:
        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        if max_chain_height != min_chain_height:
            self._logger.error('Currently only supports single block height')
            raise Exception('Currently only supports single block height')

        # get txs for this epoch
        txs_hset = await redis_conn.hgetall(epoch_txs_htable(epoch.epochId))
        all_txs = {k.decode(): EthTransactionReceipt.parse_raw(v) for k, v in txs_hset.items()}

        wallet_address = '0xae2Fc483527B8EF99EB5D9B44875F005ba1FaE13'
        wallet_txs = list(
            map(
                lambda x: x.dict(), filter(
                    lambda tx: tx.from_field == wallet_address and tx.to,
                    all_txs.values(),
                ),
            ),
        )

        snapshots = []
        for tx in wallet_txs:
            snapshots.append(
                (
                    f"{wallet_address}_{tx['to']}",
                    TrackingWalletInteractionSnapshot(
                        wallet_address=wallet_address,
                        contract_address=tx['to'],
                    ),
                ),
            )

        return snapshots
