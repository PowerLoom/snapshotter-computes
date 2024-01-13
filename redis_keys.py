from snapshotter.settings.config import settings

aave_asset_contract_data = (
    'aave:assetContract:' + settings.namespace + ':{}:AssetContractData'
)

aave_cached_block_height_asset_price = (
    'aave:assetContract:' + settings.namespace +
    ':{}:cachedAaveBlockHeightAssetPrice'
)

aave_pool_asset_list_data = (
    'aave:poolContract:assetListData'
)
