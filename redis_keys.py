from snapshotter.settings.config import settings

# Redis key for storing Uniswap pair contract token addresses
uniswap_pair_contract_tokens_addresses = (
    'uniswap:pairContract:' + settings.namespace + ':{}:PairContractTokensAddresses'
)

# Redis key for storing Uniswap pair contract token data
uniswap_pair_contract_tokens_data = (
    'uniswap:pairContract:' + settings.namespace + ':{}:PairContractTokensData'
)

# Redis key for mapping Uniswap tokens to pair contract addresses
uinswap_token_pair_contract_mapping = (
    'uniswap:tokens:' + settings.namespace + ':PairContractAddress'
)

# Redis sorted set key for storing Uniswap V2 summarized snapshots
uniswap_V2_summarized_snapshots_zset = (
    'uniswap:V2PairsSummarySnapshot:' + settings.namespace + ':snapshotsZset'
)

# Redis key for storing Uniswap V2 snapshot at a specific block height
uniswap_V2_snapshot_at_blockheight = (
    'uniswap:V2PairsSummarySnapshot:' + settings.namespace + ':snapshot:{}'
)  # {} is replaced with block_height

# Redis sorted set key for storing Uniswap V2 daily stats snapshots
uniswap_v2_daily_stats_snapshot_zset = (
    'uniswap:V2DailyStatsSnapshot:' + settings.namespace + ':snapshotsZset'
)

# Redis key for storing Uniswap V2 daily stats at a specific block height
uniswap_V2_daily_stats_at_blockheight = (
    'uniswap:V2DailyStatsSnapshot:' + settings.namespace + ':snapshot:{}'
)  # {} is replaced with block_height

# Redis sorted set key for storing Uniswap V2 tokens snapshots
uniswap_v2_tokens_snapshot_zset = (
    'uniswap:V2TokensSummarySnapshot:' + settings.namespace + ':snapshotsZset'
)

# Redis key for storing Uniswap V2 tokens at a specific block height
uniswap_V2_tokens_at_blockheight = (
    'uniswap:V2TokensSummarySnapshot:' + settings.namespace + ':{}'
)  # {} is replaced with block_height

# Redis key for caching recent logs of a Uniswap pair contract
uniswap_pair_cached_recent_logs = (
    'uniswap:pairContract:' + settings.namespace + ':{}:recentLogs'
)

# Redis key for storing the mapping of Uniswap tokens to pairs
uniswap_tokens_pair_map = (
    'uniswap:pairContract:' + settings.namespace + ':tokensPairMap'
)

# Redis key for caching token prices at specific block heights for Uniswap pairs
uniswap_pair_cached_block_height_token_price = (
    'uniswap:pairContract:' + settings.namespace +
    ':{}:cachedPairBlockHeightTokenPrice'
)

# Redis key for caching derived ETH values at specific block heights for Uniswap tokens
uniswap_token_derived_eth_cached_block_height = (
    'uniswap:token:' + settings.namespace +
    ':{}:cachedDerivedEthBlockHeight'
)
