import json
from typing import List
from typing import Tuple
from typing import Union

from redis import asyncio as aioredis

from .utils.event_log_decoder import EventLogDecoder
from .utils.models.message_models import SafeAddOwnerSnapshot
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import EthTransactionReceipt
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.redis.redis_keys import epoch_txs_htable
from snapshotter.utils.rpc import RpcHelper


class SafeAddOwnerProcessor(GenericProcessorSnapshot):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='SafeAddOwnerProcessor')

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,

    ) -> Union[None, List[Tuple[str, SafeAddOwnerSnapshot]]]:
        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        if max_chain_height != min_chain_height:
            self._logger.error('Currently only supports single block height')
            raise Exception('Currently only supports single block height')

        # TODO: we can organize transactions by topic in preloading step and that will make snapshot generation very efficient
        # using interation method for now since zkevm doesn't have a lot of transactions
        # get txs for this epoch
        txs_hset = await redis_conn.hgetall(epoch_txs_htable(epoch.epochId))
        all_txs = {k.decode(): EthTransactionReceipt.parse_raw(v) for k, v in txs_hset.items()}

        contract_address = '0xa6B71E26C5e0845f74c812102Ca7114b6a896AB2'

        # using all txns for now, can be optimized by using topic filtering
        contract_txs = list(
            map(
                lambda x: x.dict(), filter(
                    lambda tx: True,
                    all_txs.values(),
                ),
            ),
        )

        with open('snapshotter/modules/boost/static/abis/safe.json') as f:
            abi = json.load(f)

        node = rpc_helper.get_current_node()
        w3 = node['web3_client']
        contract = w3.eth.contract(address=contract_address, abi=abi)
        eld = EventLogDecoder(contract)

        snapshots = []
        processed_logs_with_initiator = []

        for tx_receipt in contract_txs:
            for log in tx_receipt['logs']:
                # AddedOwner (address owner) event topic
                if log['topics'][0] == '0x9465fa0c962cc76958e6373a993326400c1c94f8be2fe3a952adfa7f60b2ea26':
                    try:
                        decoded_log = eld.decode_log(log)
                        processed_logs_with_initiator.append(
                            (tx_receipt['from_field'].lower(), log['address'], decoded_log),
                        )
                    except:
                        pass

        if processed_logs_with_initiator:
            for initiator, safe_address, log in processed_logs_with_initiator:
                snapshots.append(
                    (
                        initiator,
                        SafeAddOwnerSnapshot(
                            safe_address=safe_address,
                            owner=log['owner'],
                        ),
                    ),
                )

        return snapshots
