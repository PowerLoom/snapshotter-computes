from snapshotter.settings.config import settings

# Stores token addresses for a specific Uniswap pair contract
uniswap_pair_contract_tokens_addresses = (
    "uniswap:pairContract:" + settings.namespace + ":{}:PairContractTokensAddresses"
)

# Stores token data for a specific Uniswap pair contract
uniswap_pair_contract_tokens_data = (
    "uniswap:pairContract:" + settings.namespace + ":{}:PairContractTokensData"
)

# Maps tokens to their corresponding Uniswap pair contract addresses
uinswap_token_pair_contract_mapping = (
    "uniswap:tokens:" + settings.namespace + ":PairContractAddress"
)

# Sorted set of Uniswap V3 summarized snapshots
uniswap_V3_summarized_snapshots_zset = (
    "uniswap:V3PairsSummarySnapshot:" + settings.namespace + ":snapshotsZset"
)

# Stores a Uniswap V3 snapshot at a specific block height
uniswap_V3_snapshot_at_blockheight = (
    "uniswap:V3PairsSummarySnapshot:" + settings.namespace + ":snapshot:{}"
)  # block_height

# Sorted set of Uniswap V3 daily stats snapshots
uniswap_V3_daily_stats_snapshot_zset = (
    "uniswap:V3DailyStatsSnapshot:" + settings.namespace + ":snapshotsZset"
)

# Stores Uniswap V3 daily stats at a specific block height
uniswap_V3_daily_stats_at_blockheight = (
    "uniswap:V3DailyStatsSnapshot:" + settings.namespace + ":snapshot:{}"
)  # block_height

# Sorted set of Uniswap V3 tokens snapshots
uniswap_V3_tokens_snapshot_zset = (
    "uniswap:V3TokensSummarySnapshot:" + settings.namespace + ":snapshotsZset"
)

# Stores Uniswap V3 tokens data at a specific block height
uniswap_V3_tokens_at_blockheight = (
    "uniswap:V3TokensSummarySnapshot:" + settings.namespace + ":{}"
)  # block_height

# Caches recent logs for a specific Uniswap pair contract
uniswap_pair_cached_recent_logs = (
    "uniswap:pairContract:" + settings.namespace + ":{}:recentLogs"
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
uniswap_pair_cached_block_height_token_price = (
    "uniswap:pairContract:" + settings.namespace + ":{}:cachedPairBlockHeightTokenPrice"
)

# Caches derived ETH values at specific block heights for Uniswap tokens
uniswap_token_derived_eth_cached_block_height = (
    "uniswap:token:" + settings.namespace + ":{}:cachedDerivedEthBlockHeight"
)

# Caches token-ETH prices at specific block heights (for multi-protocol use)
uniswap_cached_block_height_token_eth_price = (
    "uniswap:pairContract:" + settings.namespace + ":{}:cachedBlockHeightTokenEthPrice"
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
