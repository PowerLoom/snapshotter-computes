from typing import List

from pydantic import BaseModel


class UniswapContractAbis(BaseModel):
    """
    Represents the file paths for various Uniswap contract ABIs.

    Attributes:
        factory (str): Path to the Uniswap V2 Factory ABI.
        router (str): Path to the Uniswap V2 Router ABI.
        pair_contract (str): Path to the Uniswap V2 Pair ABI.
        erc20 (str): Path to the ERC20 token ABI.
        trade_events (str): Path to the Uniswap trade events ABI.
    """
    factory: str
    router: str
    pair_contract: str
    erc20: str
    trade_events: str
    pair_contract_v3: str


class ContractAddresses(BaseModel):
    """
    Represents the contract addresses for various tokens and Uniswap pairs.

    Attributes:
        iuniswap_v2_factory (str): Address of the Uniswap V2 Factory contract.
        MAKER (str): Address of the Maker token contract.
        USDbC (str): Address of the USDbC token contract.
        DAI (str): Address of the DAI token contract.
        USDC (str): Address of the USDC token contract.
        WETH (str): Address of the Wrapped Ether (WETH) token contract.
        DAI_WETH_PAIR (str): Address of the DAI-WETH pair contract.
        USDC_WETH_PAIR (str): Address of the USDC-WETH pair contract.
        USDbC_WETH_PAIR (str): Address of the USDbC-WETH pair contract.
    """
    iuniswap_v2_factory: str
    MAKER: str
    USDbC: str
    DAI: str
    USDC: str
    WETH: str
    DAI_WETH_PAIR: str
    USDC_WETH_PAIR: str
    USDbC_WETH_PAIR: str


class Settings(BaseModel):
    """
    Represents the overall settings for the Uniswap interaction.

    Attributes:
        uniswap_contract_abis (UniswapContractAbis): ABI file paths for Uniswap contracts.
        contract_addresses (ContractAddresses): Addresses of various contracts and tokens.
        uniswap_v2_whitelist (List[str]): List of whitelisted addresses for Uniswap V2.
    """
    uniswap_contract_abis: UniswapContractAbis
    contract_addresses: ContractAddresses
    uniswap_v2_whitelist: List[str]
