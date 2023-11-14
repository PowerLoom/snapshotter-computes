import json
from typing import List
from typing import Tuple
from typing import Union

from redis import asyncio as aioredis

from .utils.event_log_decoder import EventLogDecoder
from .utils.models.message_models import SafeCreateSnapshot
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import EthTransactionReceipt
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.redis.redis_keys import epoch_txs_htable
from snapshotter.utils.rpc import RpcHelper


class SafeCreateProcessor(GenericProcessorSnapshot):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='SafeCreateProcessor')

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,

    ) -> Union[None, List[Tuple[str, SafeCreateSnapshot]]]:
        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        if max_chain_height != min_chain_height:
            self._logger.error('Currently only supports single block height')
            raise Exception('Currently only supports single block height')

        # get txs for this epoch
        txs_hset = await redis_conn.hgetall(epoch_txs_htable(epoch.epochId))
        all_txs = {k.decode(): EthTransactionReceipt.parse_raw(v) for k, v in txs_hset.items()}

        contract_address = '0xa6B71E26C5e0845f74c812102Ca7114b6a896AB2'

        contract_txs = list(
            map(
                lambda x: x.dict(), filter(
                    lambda tx: tx.to == contract_address,
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
        processed_setup_logs_with_creator = []
        processed_proxy_creation_logs = []

        for tx_receipt in contract_txs:
            for log in tx_receipt['logs']:
                # SafeSetup (index_topic_1 address initiator, address[] owners, uint256 threshold, address initializer, address fallbackHandler) evnnt topic
                # and ProxyCreation (address proxy, address singleton) event topic
                if log['topics'][0] in ['0x141df868a6331af528e38c83b7aa03edc19be66e37ae67f9285bf4f8e3c6a1a8', '0x4f51faf6c4561ff95f067657e43439f0f856d97c04d9ec9070a6199ad418e235']:
                    try:
                        decoded_log = eld.decode_log(log)
                        if log['topics'][0] == '0x141df868a6331af528e38c83b7aa03edc19be66e37ae67f9285bf4f8e3c6a1a8':
                            processed_setup_logs_with_creator.append((tx_receipt['from_field'].lower(), decoded_log))
                        elif log['topics'][0] == '0x4f51faf6c4561ff95f067657e43439f0f856d97c04d9ec9070a6199ad418e235':
                            processed_proxy_creation_logs.append(decoded_log)
                    except:
                        pass

        if processed_setup_logs_with_creator and processed_proxy_creation_logs:
            for setup_log_with_creator, proxy_creation_log in zip(processed_setup_logs_with_creator, processed_proxy_creation_logs):
                creator, setup_log = setup_log_with_creator
                snapshots.append(
                    (
                        creator,
                        SafeCreateSnapshot(
                            proxy=proxy_creation_log['proxy'],
                            singleton=proxy_creation_log['singleton'],
                            initiator=setup_log['initiator'],
                            owners=setup_log['owners'],
                            threshold=setup_log['threshold'],
                            initializer=setup_log['initializer'],
                        ),
                    ),
                )

        return snapshots
