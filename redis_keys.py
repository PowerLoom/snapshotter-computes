from snapshotter.settings.config import settings

aave_asset_contract_data = (
    'aave:assetContract:' + settings.namespace + ':{}:AssetContractData'
)

aave_cached_block_height_asset_price = (
    'aave:assetContract:' + settings.namespace +
    ':{}:cachedAaveBlockHeightAssetPrice'
)

aave_cached_block_height_asset_data = (
    'aave:assetContract:' + settings.namespace +
    ':{}:cachedAaveBlockHeightAssetData'
)

aave_cached_block_height_asset_details = (
    'aave:assetContract:' + settings.namespace +
    ':{}:cachedAaveBlockHeightAssetDetails'
)

aave_cached_block_height_asset_rate_details = (
    'aave:assetContract:' + settings.namespace +
    ':{}:cachedAaveBlockHeightAssetRateDetails'
)

aave_cached_block_height_burn_mint_data = (
    'aave:assetContract:' + settings.namespace +
    ':{}:cachedAaveBlockHeightBurnMintData'
)

aave_pool_asset_list_data = (
    'aave:poolContract:' + settings.namespace + ':assetListData'
)

aave_cached_block_height_core_event_data = (
    'aave:poolContract:' + settings.namespace +
    ':cachedAaveBlockHeightEventData'
)
