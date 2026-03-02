"""
Build contract instances from an initialized RpcHelper.
Called at runtime (e.g. after rpc_helper.init()), not at import.
"""

from dataclasses import dataclass
from typing import Any

from rpc_helper.rpc import RpcHelper
from web3 import Web3

from computes.settings.config import settings as worker_settings
from computes.utils.constants import (
    factory_contract_abi,
    helper_contract_abi,
    pair_contract_abi,
)


@dataclass
class ComputesContext:
    """Holds contract instances needed for computes. Built from initialized rpc_helper."""

    helper_contract: Any
    factory_contract_obj: Any
    pair_contract_abi: list


async def build_computes_context(rpc_helper: RpcHelper) -> ComputesContext:
    """
    Build helper_contract, factory_contract_obj from initialized rpc_helper.
    Call only after await rpc_helper.init() has completed.
    """
    current_node = rpc_helper.get_current_node()
    if not current_node:
        raise RuntimeError("rpc_helper.get_current_node() returned None; ensure init() completed.")

    w3 = current_node['web3_client']

    helper_contract = w3.eth.contract(
        address=Web3.to_checksum_address(
            worker_settings.contract_addresses.uniswap_v3_helper,
        ),
        abi=helper_contract_abi,
    )
    factory_contract_obj = w3.eth.contract(
        address=Web3.to_checksum_address(
            worker_settings.contract_addresses.uniswap_v3_factory,
        ),
        abi=factory_contract_abi,
    )

    return ComputesContext(
        helper_contract=helper_contract,
        factory_contract_obj=factory_contract_obj,
        pair_contract_abi=pair_contract_abi,
    )
