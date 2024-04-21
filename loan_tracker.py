import json
from typing import List, Tuple, Union

from redis import asyncio as aioredis

from .utils.event_log_decoder import EventLogDecoder
from .utils.models.message_models import NFTLoanSnapshot
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import EthTransactionReceipt
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.redis.redis_keys import epoch_txs_htable
from snapshotter.utils.rpc import RpcHelper


class NFTLoanProcessor(GenericProcessorSnapshot):
    def __init__(self) -> None:
        self._logger = logger.bind(module='NFTLoanProcessor')

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,

    ) -> Union[None, List[Tuple[str, NFTLoanSnapshot]]]:
        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        if max_chain_height != min_chain_height:
            self._logger.error('Currently only supports single block height')
            raise Exception('Currently only supports single block height')

        # Retrieve transactions for this epoch
        txs_hset = await redis_conn.hgetall(epoch_txs_htable(epoch.epochId))
        all_txs = {k.decode(): EthTransactionReceipt.parse_raw(v) for k, v in txs_hset.items()}

        contract_address = '0x2D7D2B5fb66D414aD5dac757361139f230A92D4c'  # Replace with your NFT loan contract address Deployed !!!! 
        contract_txs = list(
            filter(
                lambda tx: tx.to == contract_address,
                all_txs.values(),
            )
        )

        with open('snapshotter/modules/computes/static/abis/nft_loan_contract.json') as f:
            abi = json.load(f)

        node = rpc_helper.get_current_node()
        w3 = node['web3_client']
        contract = w3.eth.contract(address=contract_address, abi=abi)

        eld = EventLogDecoder(contract)

        snapshots = []
        processed_logs = []

        for tx_receipt in contract_txs:
            for log in tx_receipt.logs:
                if log['topics'][0] == '0xEventSignatureHash':
                    try:
                        processed_logs.append(eld.decode_log(log))
                    except Exception as e:
                        self._logger.error(f"Error decoding log: {e}")

        if processed_logs:
            for log in processed_logs:
                snapshots.append(
                    (
                        log['borrower'].lower(),
                        NFTLoanSnapshot(
                            borrower=log['borrower'],
                            tokenId=log['tokenId'],
                            loanAmount=log['amount'],
                        ),
                    )
                )

        return snapshots

