import json
from typing import List
from typing import Tuple
from typing import Union

from redis import asyncio as aioredis

from .utils.event_log_decoder import EventLogDecoder
from .utils.helpers import safe_address_checksum
from .utils.models.message_models import QuickswapSwapSnapshot
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import EthTransactionReceipt
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.redis.redis_keys import epoch_txs_htable
from snapshotter.utils.rpc import RpcHelper


class QuickswapDAISwapProcessor(GenericProcessorSnapshot):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='QuickswapDAISwapProcessor')

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,

    ) -> Union[None, List[Tuple[str, QuickswapSwapSnapshot]]]:
        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        if max_chain_height != min_chain_height:
            self._logger.error('Currently only supports single block height')
            raise Exception('Currently only supports single block height')

        # get txs for this epoch
        txs_hset = await redis_conn.hgetall(epoch_txs_htable(epoch.epochId))
        all_txs = {k.decode(): EthTransactionReceipt.parse_raw(v) for k, v in txs_hset.items()}

        contract_addresses = [
            '0xB83B554730d29cE4Cb55BB42206c3E2c03E4A40A',
            '0xF6Ad3CcF71Abb3E12beCf6b3D2a74C963859ADCd',
        ]

        DAI_address = '0xC5015b9d9161Dca7e18e32f6f25C4aD850731Fd4'
        DAI_decimals = 18

        contract_txs = list(
            map(
                lambda x: x.dict(), filter(
                    lambda tx: safe_address_checksum(tx.to) in contract_addresses,
                    all_txs.values(),
                ),
            ),
        )

        with open('snapshotter/modules/computes/static/abis/erc20.json') as f:
            abi = json.load(f)

        node = rpc_helper.get_current_node()
        w3 = node['web3_client']
        contract = w3.eth.contract(address=contract_addresses[0], abi=abi)
        eld = EventLogDecoder(contract)

        snapshots = []
        processed_logs = []
        for tx_receipt in contract_txs:
            for log in tx_receipt['logs']:
                # Transfer (index_topic_1 address from, index_topic_2 address to, uint256 value) event topic
                if log['address'] == DAI_address and log['topics'][0] == '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef':
                    try:
                        decoded_log = eld.decode_log(log)
                        processed_logs.append(decoded_log)
                    except:
                        pass

        if processed_logs:
            for log in processed_logs:
                snapshots.append(
                    (
                        log['to'].lower(),
                        QuickswapSwapSnapshot(
                            to=log['to'],
                            value=log['value'] / (10**DAI_decimals),
                        ),
                    ),
                )

        return snapshots
