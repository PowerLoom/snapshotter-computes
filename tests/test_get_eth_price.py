import pytest
from pytest_asyncio import fixture as async_fixture
from computes.preloaders.eth_price.preloader import EthPricePreloader
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from computes.redis_keys import uniswap_eth_usd_price_zset
from snapshotter.utils.rpc import RpcHelper
from snapshotter.settings.config import settings
from fakeredis import FakeAsyncRedis

@async_fixture(scope='module')
async def rpc_helper():
    helper = RpcHelper(settings.rpc)
    await helper.init()
    yield helper

@pytest.mark.asyncio(loop_scope='module')
async def test_get_eth_price_usd(rpc_helper):
    # Create an instance of EthPricePreloader
    preloader = EthPricePreloader()

    # Use FakeAsyncRedis for caching
    fake_redis = FakeAsyncRedis()
    await fake_redis.set(source_chain_epoch_size_key(), 10)

    # Get current block number
    current_block = await rpc_helper.get_current_block_number()

    # Test parameters
    from_block = current_block - 10
    to_block = current_block - 1

    # Call the method
    result = await preloader.get_eth_price_usd(from_block, to_block, fake_redis, rpc_helper)
    print(result)

    # Assertions
    assert len(result) == 10
    assert all(isinstance(price, (int, float)) for price in result)
    assert all(price > 0 for price in result)  # Assuming ETH price is always positive

    # Verify that the result is cached in Redis
    cached_result = await fake_redis.zrangebyscore(
        name=uniswap_eth_usd_price_zset,
        min=from_block,
        max=to_block
    )
    assert cached_result

    # Clean up
    await fake_redis.flushall()
    await fake_redis.close()
