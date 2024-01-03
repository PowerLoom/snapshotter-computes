from redis import asyncio as aioredis
from snapshotter.settings.config import settings
from .utils.models.message_models import MonitoredPairsSnapshot
from snapshotter.utils.callback_helpers import GenericProcessor
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.rpc import RpcHelper
from .settings.config import settings as module_settings
from .redis_keys import uniswap_v2_monitored_pairs
from snapshotter.utils.models.message_models import EthTransactionReceipt
from snapshotter.utils.redis.redis_keys import epoch_txs_htable
from snapshotter.utils.data_utils import get_project_last_finalized_cid_and_epoch
from snapshotter.utils.data_utils import get_submission_data
from snapshotter.utils.event_log_decoder import EventLogDecoder
import json
from ipfs_client.main import AsyncIPFSClient


class FactoryMonitorProcessor(GenericProcessor):

    def __init__(self) -> None:
        self._logger = logger.bind(module='FactoryMonitorProcessor')

    async def compute(
        self,
        msg_obj: PowerloomSnapshotProcessMessage,
        redis: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
    ):

        project_id = f'pairContract_factory_monitor:{settings.namespace}'
        
        last_finalized_pairs = set()
        project_last_finalized_cid, _ = await get_project_last_finalized_cid_and_epoch(
            redis, protocol_state_contract, anchor_rpc_helper, project_id,
        )

        if project_last_finalized_cid:    
            project_last_finalized_data = await get_submission_data(
                redis, project_last_finalized_cid, ipfs_reader, project_id,
            )

            if project_last_finalized_data:
                project_last_finalized = MonitoredPairsSnapshot.parse_raw(project_last_finalized_data)
                last_finalized_pairs =  set(project_last_finalized.pairs)
                monitored_pairs = last_finalized_pairs.copy()
            else:
                monitored_pairs = await redis.smembers(uniswap_v2_monitored_pairs)
        else:
            monitored_pairs = await redis.smembers(uniswap_v2_monitored_pairs)

        # get monitored pairs from redis
        if monitored_pairs:
            monitored_pairs = set([pair.decode() for pair in monitored_pairs])

        if not monitored_pairs:
            # use initial pairs and set them in redis
            monitored_pairs = set(module_settings.initial_pairs)

        if not module_settings.static_pairs:
            # get txs for this epoch
            txs_hset = await redis.hgetall(epoch_txs_htable(msg_obj.epochId))
            all_txs = {k.decode(): EthTransactionReceipt.parse_raw(v) for k, v in txs_hset.items()}

            # factory address
            factory_address = module_settings.contract_addresses.iuniswap_v2_factory

            relevant_txns = list(
                map(
                    lambda x: x.dict(), filter(
                        lambda tx: tx.to == factory_address,
                        all_txs.values(),
                    ),
                ),
            )

            with open(module_settings.uniswap_contract_abis.factory) as f:
                abi = json.load(f)

            node = rpc_helper.get_current_node()
            w3 = node['web3_client']
            contract = w3.eth.contract(address=factory_address, abi=abi)
            eld = EventLogDecoder(contract)

            for tx_receipt in relevant_txns:
                for log in tx_receipt['logs']:
                    # PairCreated (index_topic_1 address token0, index_topic_2 address token1, address pair, uint256) event topic
                    if log['topics'][0] == '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9':
                        try:
                            decoded_log = eld.decode_log(log)
                            monitored_pairs.add(decoded_log['pair'])
                        except:
                            pass

        await redis.sadd(uniswap_v2_monitored_pairs, *monitored_pairs)

        if not last_finalized_pairs or last_finalized_pairs != monitored_pairs:
            return [("pairs", MonitoredPairsSnapshot(pairs=monitored_pairs)),]
