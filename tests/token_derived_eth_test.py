import asyncio

from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.default_logger import logger
from snapshotter.settings.config import settings
from snapshotter.modules.computes.utils.pricing import get_token_derived_eth

from ..utils.helpers import get_pair_metadata


async def test_token_derived_eth():
    
    from_block = 12084850
    to_block = from_block + 9
    rpc_helper = RpcHelper(rpc_settings=settings.rpc)
    await rpc_helper.init()
    
    pair_address = "0xe902EF54E437967c8b37D30E80ff887955c90DB6" # USDbC-WETH

    pair_metadata = await get_pair_metadata(
        rpc_helper=rpc_helper,
        pair_address=pair_address
    )

    token_metadata = pair_metadata["token1"] # USDbC

    eth_price = await get_token_derived_eth(
        rpc_helper=rpc_helper,
        from_block=from_block,
        to_block=to_block,
        token_metadata=token_metadata
    )

    from pprint import pprint
    pprint(eth_price)

if __name__ == '__main__':
    try:
        asyncio.get_event_loop().run_until_complete(test_token_derived_eth())
    except Exception as e:
        print(e)
        logger.opt(exception=True).error('exception: {}', e)
