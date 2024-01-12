from snapshotter.settings.config import settings

aave_asset_contract_data = (
    'aave:assetContract:' + settings.namespace + ':{}:AssetContractData'
)

aave_cached_block_height_asset_price = (
    'aavev3:assetContract:' + settings.namespace +
    ':{}:cachedAaveBlockHeightAssetPrice'
)
