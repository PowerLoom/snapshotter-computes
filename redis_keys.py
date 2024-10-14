from snapshotter.settings.config import settings

# Redis key for storing Aave asset contract data
aave_asset_contract_data = (
    'aave:assetContract:' + settings.namespace + ':{}:AssetContractData'
)

# Redis key for caching Aave asset price at a specific block height
aave_cached_block_height_asset_price = (
    'aave:assetContract:' + settings.namespace +
    ':{}:cachedAaveBlockHeightAssetPrice'
)

# Redis key for caching Aave asset data at a specific block height
aave_cached_block_height_asset_data = (
    'aave:assetContract:' + settings.namespace +
    ':{}:cachedAaveBlockHeightAssetData'
)

# Redis key for caching Aave asset details at a specific block height
aave_cached_block_height_asset_details = (
    'aave:assetContract:' + settings.namespace +
    ':{}:cachedAaveBlockHeightAssetDetails'
)

# Redis key for caching Aave asset rate details at a specific block height
aave_cached_block_height_asset_rate_details = (
    'aave:assetContract:' + settings.namespace +
    ':{}:cachedAaveBlockHeightAssetRateDetails'
)

# Redis key for storing Aave pool asset set data
aave_pool_asset_set_data = (
    'aave:poolContract:' + settings.namespace + ':assetSetData'
)

# Redis key for caching Aave core event data at a specific block height
aave_cached_block_height_core_event_data = (
    'aave:poolContract:' + settings.namespace +
    ':cachedAaveBlockHeightEventData'
)

# Redis key for caching Aave assets prices at a specific block height
aave_cached_block_height_assets_prices = (
    'aave:poolContract:' + settings.namespace +
    ':cachedAaveBlockHeightAssetsPrices'
)
