from snapshotter.settings.config import settings

aave_asset_contract_data = (
    'aave:assetContract:' + settings.namespace + ':{}:AssetContractData'
)

uniswap_pair_cached_block_height_token_price = (
    'uniswap:pairContract:' + settings.namespace +
    ':{}:cachedPairBlockHeightTokenPrice'
)

uniswap_cached_block_height_token_eth_price = (
    'uniswap:pairContract:' + settings.namespace + ':{}:cachedBlockHeightTokenEthPrice'
)
