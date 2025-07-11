from snapshotter.settings.config import settings

# Stores token addresses for a specific Uniswap pair contract
uniswap_pair_contract_tokens_addresses = (
    "uniswap:pairContract:" + settings.namespace + ":{}:PairContractTokensAddresses"
)

# Stores token data for a specific Uniswap pair contract
uniswap_pair_contract_tokens_data = (
    "uniswap:pairContract:" + settings.namespace + ":{}:PairContractTokensData"
)

# Maps tokens to their corresponding Uniswap pairs
uniswap_tokens_pair_map = (
    "uniswap:pairContract:" + settings.namespace + ":tokensPairMap"
)

# Maps ticks to their corresponding Uniswap pairs
uniswap_ticks_pair_map = (
    "uniswap:pairContract:" + settings.namespace + ":ticksPairMap"
)

# Caches token prices at specific block heights for Uniswap pairs
uniswap_pool_cached_block_height_token_price = (
    "uniswap:pairContract:" + settings.namespace + ":{}:cachedPairBlockHeightTokenPrice"
)

# Caches token-ETH prices at specific block heights (for multi-protocol use)
uniswap_cached_block_height_token_eth_price = (
    "uniswap:pairContract:" + settings.namespace + ":{}:cachedBlockHeightTokenEthPrice"
)

# Caches token-ETH prices at specific block heights (for multi-protocol use)
uniswap_pool_cached_block_height_token_price_raw = (
    "uniswap:pairContract:" + settings.namespace + ":{}:cachedBlockHeightTokenPriceRaw"
)

# Caches tick data at specific block heights for Uniswap pairs
uniswap_cached_tick_data_block_height = (
    "uniswap:pairContract:" + settings.namespace + ":{}:cachedBlockHeightTickData"
)

# Caches reserves at specific block heights for Uniswap pairs
uniswap_pair_cached_block_height_reserves = (
    "uniswap:pairContract:" + settings.namespace + ":{}:cachedBlockHeightReserves"
)

# Stores the list of monitored Uniswap V3 pairs for snapshotting
uniswap_v3_monitored_pairs = 'uniswap:monitoredPairs'

# Maps tokens to their best Uniswap V3 pairs based on liquidity
uniswap_v3_best_pair_map = (
    "uniswap:pairContract:" + settings.namespace + ":bestPairMap"
)

# Maps tokens to their stable Uniswap V3 pairs
uniswap_v3_token_stable_pair_map = (
    f"uniswap:pairContract:" + settings.namespace + ":{}:tokenStablePairMap"
)

# Redis key for Uniswap ETH/USD price data
uniswap_eth_usd_price_zset = (
    'uniswap:ethBlockHeightPrice:' + settings.namespace + ':ethPriceZset'
)

# Redis key for Uniswap pool metadata
def get_pool_metadata_key(pool_address: str) -> str:
    return f'pool_metadata:{pool_address}'

# Redis key for Uniswap best pair map
uniswap_v3_best_pool_map = (
    "uniswap:pairContract:" + settings.namespace + ":bestPoolMap"
)
