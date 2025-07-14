"""
This module contains the adapted API endpoints that parse specific computes made by the Uniswap V3 compute modules
This router is designed to be included in the core API application.
"""

from fastapi import APIRouter
from fastapi import Request
from fastapi import Response
from fastapi import Query
from typing import Optional
from web3 import Web3

from computes.utils.models.message_models import UniswapBaseSnapshot
from snapshotter.settings.config import settings
from computes.api.utils.data_utils import (
    get_uniswap_trade_volume_agg,
    get_uniswap_v3_base_snapshot,
    get_uniswap_v3_eth_price_snapshot,
    get_uniswap_price_series_agg,
    get_uniswap_v3_token_pools_snapshot, 
    get_uniswap_v3_token_price_pool, 
    get_uniswap_v3_token_prices_all_snapshot, 
    get_uniswap_v3_trades_snapshot,
    get_uniswapv3_snapshot,
    get_uniswap_v3_pool_metadata,
    get_uniswap_v3_pool_trades,
    get_uniswap_v3_base_snapshots_for_token,
    get_uniswap_trade_volume_agg_all_pools,
    get_active_pools,
    get_active_tokens,
    get_uniswap_v3_all_trades_snapshot,
)
from snapshotter.utils.default_logger import default_logger
from computes.settings.config import settings as compute_settings

rest_logger = default_logger.bind(module='UniswapV3API')

# Create APIRouter instead of FastAPI app
router = APIRouter(tags=["uniswap"])


@router.get('/pool/{pool_address}/metadata')
async def get_pool_metadata(
    pool_address: str,
    request: Request,
    response: Response,
):
    """
    Get the metadata for a specific pool.
    """
    pool_address = Web3.to_checksum_address(pool_address)
    # TODO: integrate pool metadata fetch logic from compute module
    try:
        pool_metadata = await get_uniswap_v3_pool_metadata(
            redis_conn=request.app.state.redis_conn,
            protocol_state_contract=request.app.state.protocol_state_contract,
            anchor_rpc_helper=request.app.state.anchor_rpc_helper,
            ipfs_reader=request.app.state.ipfs_reader_client,
            pool_address=pool_address,
        )
        if not pool_metadata:
            response.status_code = 404
            return {"error": "Pool metadata not found"}
        else:
            response.status_code = 200
            return pool_metadata
    except Exception as e:
        rest_logger.error(f"Error getting pool metadata for {pool_address}: {e}")
        response.status_code = 500
        return {"error": "Pool metadata not found"}


@router.get('/token/{token_address}/pools')
async def get_token_pools(
    token_address: str,
    request: Request,
    response: Response,
):
    """
    Get the token pools for a specific token.
    """
    token_address = Web3.to_checksum_address(token_address)
    # TODO: integrate token pools fetch logic from compute module
    try:
        token_pools_snapshot = await get_uniswap_v3_token_pools_snapshot(
            redis_conn=request.app.state.redis_conn,
            protocol_state_contract=request.app.state.protocol_state_contract,
            anchor_rpc_helper=request.app.state.anchor_rpc_helper,
            ipfs_reader=request.app.state.ipfs_reader_client,
            token_address=token_address,
        )
        if not token_pools_snapshot:
            response.status_code = 404
            return {"error": "Token pools not found"}
        else:
            response.status_code = 200
            return token_pools_snapshot
    except Exception as e:
        rest_logger.error(f"Error getting token pools for {token_address}: {e}")
        response.status_code = 500
        return {"error": "Token pools not found"}


@router.get('/ethPrice/{block_number}')
@router.get('/ethPrice')
async def get_ethprice(
    request: Request,
    response: Response,
    block_number: Optional[int] = None,
):
    """
    Get ETH price snapshot for a specific block number or latest finalized epoch.
    
    Args:
        request: FastAPI request object
        response: FastAPI response object
        block_number: Optional block number to get ETH price for. If not provided, uses latest finalized epoch.
    """
    try:
        eth_price_snapshot = await get_uniswap_v3_eth_price_snapshot(
            redis_conn=request.app.state.redis_conn,
            protocol_state_contract=request.app.state.protocol_state_contract,
            anchor_rpc_helper=request.app.state.anchor_rpc_helper,
            ipfs_reader=request.app.state.ipfs_reader_client,
            block_number=block_number,
        )
        if not eth_price_snapshot:
            response.status_code = 404
            return {"error": "ETH price snapshot not found"}
        else:
            response.status_code = 200
            return eth_price_snapshot
    except Exception as e:
        rest_logger.error(f"Error getting ETH price snapshot: {e}")
        response.status_code = 500
        return {"error": "ETH price snapshot not found"}


@router.get('/token/price/{token_address}/{pool_address}')
@router.get('/token/price/{token_address}/{pool_address}/{block_number}')
async def get_token_price_pool(
    request: Request,
    response: Response,
    token_address: str,
    pool_address: Optional[str] = None,
    block_number: Optional[int] = None,
):
    try:
        token_price = await get_uniswap_v3_token_price_pool(
            redis_conn=request.app.state.redis_conn,
            protocol_state_contract=request.app.state.protocol_state_contract,
            anchor_rpc_helper=request.app.state.anchor_rpc_helper,
            ipfs_reader=request.app.state.ipfs_reader_client,
            token_address=token_address,
            pool_address=pool_address,
            block_number=block_number,
        )
        if not token_price:
            response.status_code = 404
            return {"error": "Token price snapshot not found"}
        else:
            response.status_code = 200
            return token_price
    except Exception as e:
        rest_logger.error(
            f"Error getting token price snapshot for {token_address} "
            f"in pool {pool_address} at block {block_number}: {e}"
        )
        response.status_code = 500
        return {"error": "Token price snapshot not found"}
    

@router.get('/snapshot/base_all_pools/{token_address}')
async def get_token_base_snapshots(
    request: Request,
    response: Response,
    token_address: str,
):
    token_address = Web3.to_checksum_address(token_address)
    tokens_to_ignore = [compute_settings.contract_addresses.WETH]
    if token_address in tokens_to_ignore:
        response.status_code = 400
        return {"error": "Invalid token address"}
    
    try:
        base_snapshots = await get_uniswap_v3_base_snapshots_for_token(
            redis_conn=request.app.state.redis_conn,
            anchor_rpc_helper=request.app.state.anchor_rpc_helper,
            ipfs_reader=request.app.state.ipfs_reader_client,
            protocol_state_contract=request.app.state.protocol_state_contract,
            token_address=token_address,
        )
        if not base_snapshots:
            response.status_code = 404
            return {"error": "Base snapshots not found"}
        else:
            response.status_code = 200
            return base_snapshots
    except Exception as e:
        rest_logger.error(f"Error getting base snapshots for {token_address}: {e}")
        response.status_code = 500
        return {"error": "Base snapshots not found"}


@router.get('/snapshot/base/{pool_address}')
@router.get('/snapshot/base/{pool_address}/{block_number}')
async def get_base_snapshot(
    request: Request,
    response: Response,
    pool_address: str,
    block_number: Optional[int] = None,
):
    pool_address = Web3.to_checksum_address(pool_address)
    try:
        base_snapshot = await get_uniswap_v3_base_snapshot(
            redis_conn=request.app.state.redis_conn,
            protocol_state_contract=request.app.state.protocol_state_contract,
            anchor_rpc_helper=request.app.state.anchor_rpc_helper,
            ipfs_reader=request.app.state.ipfs_reader_client,
            pool_address=pool_address,
            block_number=block_number,
        )
        if not base_snapshot:
            response.status_code = 404
            return {"error": "Base snapshot not found"}
        else:
            response.status_code = 200
            return base_snapshot
    except Exception as e:
        rest_logger.error(f"Error getting base snapshot for {pool_address} at block {block_number}: {e}")
        response.status_code = 500
        return {"error": "Base snapshot not found"}
    

@router.get('/snapshot/trades/{pool_address}')
@router.get('/snapshot/trades/{pool_address}/{block_number}')
async def get_trades_snapshot(
    request: Request,
    response: Response,
    pool_address: str,
    block_number: Optional[int] = None,
):
    pool_address = Web3.to_checksum_address(pool_address)
    try:
        trades_snapshot = await get_uniswap_v3_trades_snapshot(
            redis_conn=request.app.state.redis_conn,
            protocol_state_contract=request.app.state.protocol_state_contract,
            anchor_rpc_helper=request.app.state.anchor_rpc_helper,
            ipfs_reader=request.app.state.ipfs_reader_client,
            pool_address=pool_address,
            block_number=block_number,
        )
        if not trades_snapshot:
            response.status_code = 404
            return {"error": "Trades snapshot not found"}
        else:
            response.status_code = 200
            return trades_snapshot
    except Exception as e:
        rest_logger.error(f"Error getting trades snapshot for {pool_address} at block {block_number}: {e}")
        response.status_code = 500
        return {"error": "Trades snapshot not found"}


@router.get('/snapshot/allTrades')
@router.get('/snapshot/allTrades/{block_number}')
async def get_all_trades_snapshot(
    request: Request,
    response: Response,
    block_number: Optional[int] = None,
):
    try:
        trades_snapshot = await get_uniswap_v3_all_trades_snapshot(
            redis_conn=request.app.state.redis_conn,
            protocol_state_contract=request.app.state.protocol_state_contract,
            anchor_rpc_helper=request.app.state.anchor_rpc_helper,
            ipfs_reader=request.app.state.ipfs_reader_client,
            block_number=block_number,
        )
        if not trades_snapshot:
            response.status_code = 404
            return {"error": "Trades snapshot not found"}
        else:
            response.status_code = 200
            return trades_snapshot
    except Exception as e:
        rest_logger.error(f"Error getting trades snapshot for all pools at block {block_number}: {e}")
        response.status_code = 500
        return {"error": "Trades snapshot not found"}


@router.get('/tokenPrices/all/{token_address}')
@router.get('/tokenPrices/all/{token_address}/{block_number}')
async def get_token_price_all(
    request: Request,
    response: Response,
    token_address: str,
    block_number: Optional[int] = None,
):
    try:
        token_prices = await get_uniswap_v3_token_prices_all_snapshot(
            redis_conn=request.app.state.redis_conn,
            protocol_state_contract=request.app.state.protocol_state_contract,
            anchor_rpc_helper=request.app.state.anchor_rpc_helper,
            ipfs_reader=request.app.state.ipfs_reader_client,
            token_address=token_address,
            block_number=block_number,
        )
        if not token_prices:
            response.status_code = 404
            return {"error": "Token price snapshot not found"}
        else:
            response.status_code = 200
            return token_prices
    except Exception as e:
        rest_logger.error(f"Error getting token price snapshot for {token_address} at block {block_number}: {e}")
        response.status_code = 500
        return {"error": "Token price snapshot not found"}


@router.get('/tradeVolumeAllPools/{token_address}/{time_interval}')
async def get_trade_volume_agg_all_pools(
    request: Request,
    response: Response,
    token_address: str,
    time_interval: int,
):  
    
    try:
        trade_volume_agg = await get_uniswap_trade_volume_agg_all_pools(
            redis_conn=request.app.state.redis_conn,
            anchor_rpc_helper=request.app.state.anchor_rpc_helper,
            ipfs_reader=request.app.state.ipfs_reader_client,
            protocol_state_contract=request.app.state.protocol_state_contract,
            time_interval=time_interval,
            token_address=token_address,
        )
    except Exception as e:
        if "Too many pools found for token" in str(e):
            response.status_code = 400
            return {"error": f"Too many pools found for Token: {token_address}, aggregation over volume not supported yet! Please use the tradeVolume/{pool_address}/{time_interval} endpoint instead."}
        rest_logger.opt(exception=True).error(f"Error getting trade volume agg for {token_address}: {e}")
        response.status_code = 500
        return {"error": "Trade volume agg not found"}
    if not trade_volume_agg:
        response.status_code = 404
        return {"error": "Trade volume agg not found"}
    else:
        response.status_code = 200
        return trade_volume_agg


@router.get('/tradeVolume/{pool_address}/{time_interval}')
async def get_trade_volume_agg(
    request: Request,
    response: Response,
    pool_address: str,
    time_interval: int,
):
    pool_address = Web3.to_checksum_address(pool_address)
    project_id = f"baseSnapshot:{pool_address}:{settings.namespace}"
    try:
        trade_volume_agg = await get_uniswap_trade_volume_agg(
            redis_conn=request.app.state.redis_conn,
            protocol_state_contract=request.app.state.protocol_state_contract,
            anchor_rpc_helper=request.app.state.anchor_rpc_helper,
            ipfs_reader=request.app.state.ipfs_reader_client,
            project_id=project_id,
            time_interval=time_interval,
        )
    except Exception as e:
        rest_logger.opt(exception=True).error(f"Error getting trade volume agg for {pool_address}: {e}")
        response.status_code = 500
        return {"error": "Trade volume agg not found"}
    if not trade_volume_agg:
        response.status_code = 404
        return {"error": "Trade volume agg not found"}
    else:
        response.status_code = 200
        return trade_volume_agg
    

@router.get('/poolTrades/{pool_address}/{start_timestamp}/{end_timestamp}')
async def get_pool_trades(
    request: Request,
    response: Response,
    pool_address: str,
    start_timestamp: int,
    end_timestamp: int,
):
    pool_address = Web3.to_checksum_address(pool_address)
    project_id = f"tradesSnapshot:{pool_address}:{settings.namespace}"
    try:
        pool_trades = await get_uniswap_v3_pool_trades(
            redis_conn=request.app.state.redis_conn,
            anchor_rpc_helper=request.app.state.anchor_rpc_helper,
            rpc_helper=request.app.state.rpc_helper,
            ipfs_reader=request.app.state.ipfs_reader_client,
            project_id=project_id,
            pool_address=pool_address,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            protocol_state_contract=request.app.state.protocol_state_contract,
        )
    except Exception as e:
        rest_logger.opt(exception=True).error(f"Error getting pool trades for {pool_address}: {e}")
        response.status_code = 500
        return {"error": "Pool trades not found"}
    
    if not pool_trades:
        response.status_code = 404
        return {"error": "Pool trades not found"}
    else:
        response.status_code = 200
        return pool_trades


@router.get('/timeSeries/{token_address}/{pool_address}/{time_interval}/{step_seconds}')
async def get_token_price_series(
    request: Request,
    response: Response,
    token_address: str,
    pool_address: str,
    time_interval: int,
    step_seconds: int,
):
    token_address = Web3.to_checksum_address(token_address)
    pool_address = Web3.to_checksum_address(pool_address)
    project_id = f"baseSnapshot:{pool_address}:{settings.namespace}"
    try:
        token_price_series = await get_uniswap_price_series_agg(
            redis_conn=request.app.state.redis_conn,
            protocol_state_contract=request.app.state.protocol_state_contract,
            rpc_helper=request.app.state.rpc_helper,
            anchor_rpc_helper=request.app.state.anchor_rpc_helper,
            ipfs_reader=request.app.state.ipfs_reader_client,
            token_address=token_address,
            time_interval=time_interval,
            project_id=project_id,
            step_seconds=step_seconds,
        )
        if not token_price_series:
            response.status_code = 404
            return {"error": "Token price series not found"}
        else:
            response.status_code = 200
            return token_price_series
    except Exception as e:
        rest_logger.error(f"Error getting token price series for {token_address}: {e}")
        response.status_code = 500
        return {"error": "Token price series not found"}


@router.get(
    '/dailyActiveTokens',
    summary="Get daily active tokens with pagination",
    description=(
        "Retrieves a paginated list of active tokens for the current day, "
        "sorted by frequency. Use page and size parameters to control pagination."
    ),
    response_description="Returns a paginated list of active tokens with their frequencies"
)
async def get_daily_active_tokens(
    request: Request,
    response: Response,
    page: int = Query(
        default=1,
        ge=1,
        description="Page number to retrieve (starts at 1)",
        example=1
    ),
    size: int = Query(
        default=50,
        ge=1,
        le=100,
        description="Number of items per page (max 100)",
        example=50
    ),
    metadata: bool = Query(
        default=False,
        description="Include token metadata in the response",
        example=False
    ),
    time_interval: int = Query(
        default=86400,
        description="Time interval in seconds",
        example=86400
    ),
):
    """
    Get a paginated list of active tokens for the current day.
    
    Parameters:
    - page: The page number to retrieve (starts at 1)
    - size: Number of items per page (default: 50, max: 100)
    - metadata: Include token metadata in the response (default: False)
    
    Returns:
    - List of active tokens with their frequencies and optional metadata
    - Pagination metadata including total count and pages
    """
    
    try:
        tokens_data, total_tokens = await get_active_tokens(
            redis_conn=request.app.state.redis_conn,
            protocol_state_contract=request.app.state.protocol_state_contract,
            anchor_rpc_helper=request.app.state.anchor_rpc_helper,
            ipfs_reader=request.app.state.ipfs_reader_client,
            time_interval=time_interval,
            page=page,
            size=size,
            metadata=metadata,
        )
        
        response.status_code = 200
        return {
            "active_tokens": tokens_data,
            "pagination": {
                "page": page,
                "size": size,
                "total": total_tokens,
                "total_pages": (total_tokens + size - 1) // size
            }
        }
    except Exception as e:
        rest_logger.opt(exception=True).error(f"Error getting daily active tokens: {e}")
        response.status_code = 500
        return {"error": "Failed to retrieve daily active tokens"}
    

@router.get(
    '/dailyActivePools', 
    summary="Get daily active pools with pagination",
    description=(
        "Retrieves a paginated list of active pools for the current day, "
        "sorted by frequency. Use page and size parameters to control pagination."
    ),
    response_description="Returns a paginated list of active pools with their frequencies"
)
async def get_daily_active_pools(
    request: Request,
    response: Response,
    page: int = Query(
        default=1,
        ge=1,
        description="Page number to retrieve (starts at 1)",
        example=1
    ),
    size: int = Query(
        default=50,
        ge=1,
        le=100,
        description="Number of items per page (max 100)",
        example=50
    ),
    metadata: bool = Query(
        default=False,
        description="Include pool metadata in the response",
        example=False
    ),
    time_interval: int = Query(
        default=86400,
        description="Time interval in seconds",
        example=86400
    ),
):
    """
    Get a paginated list of active pools for the current day.
    
    Parameters:
    - page: The page number to retrieve (starts at 1)
    - size: Number of items per page (default: 50, max: 100)
    - metadata: Include pool metadata in the response (default: False)
    
    Returns:
    - List of active pools with their frequencies and optional metadata
    - Pagination metadata including total count and pages
    """
    try:
        pools_data, total_pools = await get_active_pools(
            redis_conn=request.app.state.redis_conn,
            protocol_state_contract=request.app.state.protocol_state_contract,
            anchor_rpc_helper=request.app.state.anchor_rpc_helper,
            ipfs_reader=request.app.state.ipfs_reader_client,
            time_interval=time_interval,
            page=page,
            size=size,
            metadata=metadata,
        )
    
        response.status_code = 200
        return {
            "active_pools": pools_data,
            "pagination": {
                "page": page,
                "size": size,
                "total": total_pools,
                "total_pages": (total_pools + size - 1) // size
            }
        }
    except Exception as e:
        rest_logger.error(f"Error getting daily active pools: {e}")
        response.status_code = 500
        return {"error": "Failed to retrieve daily active pools"}


@router.get(
    '/poolData/{pool_address}/{block_number}', 
    summary='Returns the base snapshot for a given pool address and block number'
)
@router.get(
    '/poolData/{pool_address}', 
    summary='Returns the base snapshot for a given pool address and last finalized epoch/block number'
)
async def get_pool_data(
    request: Request,
    response: Response,
    pool_address: str,
    block_number: Optional[int] = None,
):
    pool_address = Web3.to_checksum_address(pool_address)
    result = await get_uniswapv3_snapshot(
        redis_conn=request.app.state.redis_conn,
        anchor_rpc_helper=request.app.state.anchor_rpc_helper,
        ipfs_reader=request.app.state.ipfs_reader_client,
        protocol_state_contract=request.app.state.protocol_state_contract,
        project_id=f"baseSnapshot:{pool_address.lower()}:{settings.namespace}",
        message_model=UniswapBaseSnapshot,
        block_number=block_number,
    )
    if not result:
        response.status_code = 404
        return {"error": "Base snapshot not found"}
    else:
        base_snapshot = result[1]
        response.status_code = 200
        return base_snapshot
    