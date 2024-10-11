from pydantic import BaseModel


class UniswapContractAbis(BaseModel):
    factory: str
    router: str
    quoter: str
    multicall: str
    pair_contract: str
    erc20: str
    trade_events: str
    quoter_1inch: str


class ContractAddresses(BaseModel):
    uniswap_v3_factory: str
    uniswap_v3_router: str
    uniswap_v3_quoter: str
    uniswap_v3_multicall: str
    DAI_WETH_PAIR: str
    USDC_WETH_PAIR: str
    USDT_WETH_PAIR: str
    WETH: str
    MAKER: str
    QUOTER_1INCH: str
    USDC: str
    USDT: str
    DAI: str


class Settings(BaseModel):
    uniswap_contract_abis: UniswapContractAbis
    contract_addresses: ContractAddresses
