from snapshotter.settings.config import settings

def uniswap_pair_contract_tokens_addresses_key(namespace: str, pool_address: str) -> str:
    """
    Generate Redis key for Uniswap pair contract token addresses.
    """
    return f"uniswap:pairContract:{namespace}:{pool_address}:PairContractTokensAddresses"

def uniswap_pair_contract_tokens_data_key(namespace: str, pool_address: str) -> str:
    """
    Generate Redis key for Uniswap pair contract token data.
    """
    return f"uniswap:pairContract:{namespace}:{pool_address}:PairContractTokensData"

def uniswap_tokens_pair_map_key(namespace: str) -> str:
    """
    Generate Redis key for mapping tokens to their corresponding Uniswap pairs.
    """
    return f"uniswap:pairContract:{namespace}:tokensPairMap"

def uniswap_ticks_pair_map_key(namespace: str) -> str:
    """
    Generate Redis key for mapping ticks to their corresponding Uniswap pairs.
    """
    return f"uniswap:pairContract:{namespace}:ticksPairMap"

def uniswap_pair_cached_block_height_token_price_key(namespace: str, pool_address: str) -> str:
    """
    Generate Redis key for caching token prices at specific block heights for Uniswap pairs.
    """
    return f"uniswap:pairContract:{namespace}:{pool_address}:cachedPairBlockHeightTokenPrice"

def uniswap_cached_block_height_token_eth_price_key(namespace: str, token_address: str) -> str:
    """
    Generate Redis key for caching token-ETH prices at specific block heights.
    """
    return f"uniswap:pairContract:{namespace}:{token_address}:cachedBlockHeightTokenEthPrice"

def uniswap_cached_tick_data_block_height_key(namespace: str, pool_address: str) -> str:
    """
    Generate Redis key for caching tick data at specific block heights for Uniswap pairs.
    """
    return f"uniswap:pairContract:{namespace}:{pool_address}:cachedBlockHeightTickData"

def uniswap_pair_cached_block_height_reserves_key(namespace: str, pool_address: str) -> str:
    """
    Generate Redis key for caching reserves at specific block heights for Uniswap pairs.
    """
    return f"uniswap:pairContract:{namespace}:{pool_address}:cachedBlockHeightReserves"

def uniswap_v3_monitored_pairs_key() -> str:
    """
    Generate Redis key for the list of monitored Uniswap V3 pairs for snapshotting.
    """
    return 'uniswap:monitoredPairs'

def uniswap_v3_best_pair_map_key(namespace: str) -> str:
    """
    Generate Redis key for mapping tokens to their best Uniswap V3 pairs based on liquidity.
    """
    return f"uniswap:pairContract:{namespace}:bestPairMap"

def uniswap_v3_token_stable_pair_map_key(namespace: str, token_address: str) -> str:
    """
    Generate Redis key for mapping tokens to their stable Uniswap V3 pairs.
    """
    return f"uniswap:pairContract:{namespace}:{token_address}:tokenStablePairMap"

def uniswap_eth_usd_price_zset_key(namespace: str) -> str:
    """
    Generate Redis key for Uniswap ETH/USD price data sorted set.
    """
    return f'uniswap:ethBlockHeightPrice:{namespace}:ethPriceZset'

def pool_metadata_key(pool_address: str) -> str:
    """
    Generate Redis key for Uniswap pool metadata.
    """
    return f'pool_metadata:{pool_address}'

def active_pools_per_block_key(block_number: int, namespace: str) -> str:
    """
    Generate Redis key for active pools per block.
    """
    return f"active_pools_per_block:{block_number}:{namespace}"

def active_tokens_per_block_key(block_number: int, namespace: str) -> str:
    """
    Generate Redis key for active tokens per block.
    """
    return f"active_tokens_per_block:{block_number}:{namespace}"

def events_by_pool_address_key(namespace: str, pool_address: str) -> str:
    """
    Generate Redis key for events by pool address.
    """
    return f"events:{namespace}:address:{pool_address}"
