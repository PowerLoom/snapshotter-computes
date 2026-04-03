"""
Uniswap V3 API Router

This module defines FastAPI endpoints for accessing Uniswap V3 compute module data.
It provides endpoints for pool and token metadata, price and trade snapshots, time series,
trade volume aggregations, and daily active tokens/pools with pagination.

This router is designed to be included in the core API application.
"""

import asyncio
import json

from fastapi import APIRouter
from fastapi import Request
from fastapi import Response
from fastapi import Query
from typing import Optional
from web3 import Web3
from starlette.responses import StreamingResponse

from snapshotter.settings.config import settings
from snapshotter.utils.redis.redis_keys import project_last_finalized_epoch_hmap
from snapshotter.utils.redis.redis_keys import snapshot_finalized_channel
from computes.api.utils.data_utils import (
    get_uniswap_trade_volume_agg,
    get_uniswap_v3_base_snapshot,
    get_uniswap_v3_eth_price_snapshot,
    get_uniswap_price_series_agg,
    get_uniswap_v3_token_pools_snapshot, 
    get_uniswap_v3_token_price_pool, 
    get_uniswap_v3_token_prices_all_snapshot, 
    get_uniswap_v3_trades_snapshot,
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

# Bind logger for this module
rest_logger = default_logger.bind(module='UniswapV3API')

# SSE /mpp/stream/allTrades: retries per epoch before skipping (null CID, empty snapshot, transient IPFS).
MPP_STREAM_SSE_FETCH_ATTEMPTS_PER_EPOCH = 5

# Create APIRouter for Uniswap endpoints
router = APIRouter(tags=["uniswap"])


def _snapshot_model_to_json(obj):
    if obj is None:
        return None
    if hasattr(obj, 'model_dump'):
        return obj.model_dump(mode='json')
    return obj


async def _anchor_current_epoch_id(request: Request) -> int:
    ps = request.app.state.protocol_state_contract
    [current_epoch_data] = await request.app.state.anchor_rpc_helper.web3_call(
        tasks=[
            ('currentEpoch', [Web3.to_checksum_address(settings.data_market)]),
        ],
        contract_addr=ps.address,
        abi=ps.abi,
    )
    return int(current_epoch_data[2])


@router.get('/pool/{pool_address}/metadata')
async def get_pool_metadata(
    pool_address: str,
    request: Request,
    response: Response,
):
    """
    Retrieve metadata for a specific Uniswap V3 pool.

    Args:
        pool_address (str): The address of the pool.
        request (Request): FastAPI request object.
        response (Response): FastAPI response object.

    Returns:
        dict: Pool metadata or error message.
    """
    pool_address = Web3.to_checksum_address(pool_address)
    # Fetch pool metadata using compute module utility
    try:
        pool_metadata = await get_uniswap_v3_pool_metadata(
            redis_conn=request.app.state.redis_conn,
            protocol_state_contract=request.app.state.protocol_state_contract,
            rpc_helper=request.app.state.rpc_helper,
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
        return {"error": str(e)}


@router.get('/token/{token_address}/pools')
async def get_token_pools(
    token_address: str,
    request: Request,
    response: Response,
):
    """
    Retrieve all pools associated with a specific token.

    Args:
        token_address (str): The address of the token.
        request (Request): FastAPI request object.
        response (Response): FastAPI response object.

    Returns:
        dict: List of pools or error message.
    """
    token_address = Web3.to_checksum_address(token_address)
    # Fetch token pools using compute module utility
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
        return {"error": str(e)}


@router.get('/ethPrice/{block_number}')
@router.get('/ethPrice')
async def get_ethprice(
    request: Request,
    response: Response,
    block_number: Optional[int] = None,
):
    """
    Retrieve the ETH price snapshot for a specific block number or the latest finalized epoch.

    Args:
        request (Request): FastAPI request object.
        response (Response): FastAPI response object.
        block_number (Optional[int]): Block number to get ETH price for. If not provided, uses latest.

    Returns:
        dict: ETH price snapshot or error message.
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
        return {"error": str(e)}


@router.get('/token/price/{token_address}/{pool_address}')
@router.get('/token/price/{token_address}/{pool_address}/{block_number}')
async def get_token_price_pool(
    request: Request,
    response: Response,
    token_address: str,
    pool_address: str,
    block_number: Optional[int] = None,
):
    """
    Retrieve the price of a token in a specific pool, optionally at a specific block.

    Args:
        request (Request): FastAPI request object.
        response (Response): FastAPI response object.
        token_address (str): Token address.
        pool_address (str): Pool address.
        block_number (Optional[int]): Block number (optional).

    Returns:
        dict: Token price snapshot or error message.
    """
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
        return {"error": str(e)}
    

@router.get('/snapshot/base_all_pools/{token_address}')
async def get_token_base_snapshots(
    request: Request,
    response: Response,
    token_address: str,
):
    """
    Retrieve base snapshots for all pools associated with a given token.

    Args:
        request (Request): FastAPI request object.
        response (Response): FastAPI response object.
        token_address (str): Token address.

    Returns:
        dict: Base snapshots or error message.
    """
    token_address = Web3.to_checksum_address(token_address)
    tokens_to_ignore = [compute_settings.contract_addresses.WETH]
    # Prevent querying for ignored tokens (e.g., WETH)
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
        return {"error": str(e)}


@router.get('/snapshot/base/{pool_address}')
@router.get('/snapshot/base/{pool_address}/{block_number}')
@router.get('/mpp/snapshot/base/{pool_address}', tags=['uniswap', 'mpp'])
@router.get('/mpp/snapshot/base/{pool_address}/{block_number}', tags=['uniswap', 'mpp'])
async def get_base_snapshot(
    request: Request,
    response: Response,
    pool_address: str,
    block_number: Optional[int] = None,
):
    """
    Retrieve the base snapshot for a specific pool, optionally at a specific block.

    Args:
        request (Request): FastAPI request object.
        response (Response): FastAPI response object.
        pool_address (str): Pool address.
        block_number (Optional[int]): Block number (optional).

    Returns:
        dict: Base snapshot or error message.
    """
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
        return {"error": str(e)}
    

@router.get('/snapshot/trades/{pool_address}')
@router.get('/snapshot/trades/{pool_address}/{block_number}')
@router.get('/mpp/snapshot/trades/{pool_address}', tags=['uniswap', 'mpp'])
@router.get('/mpp/snapshot/trades/{pool_address}/{block_number}', tags=['uniswap', 'mpp'])
async def get_trades_snapshot(
    request: Request,
    response: Response,
    pool_address: str,
    block_number: Optional[int] = None,
):
    """
    Retrieve the trades snapshot for a specific pool, optionally at a specific block.

    Args:
        request (Request): FastAPI request object.
        response (Response): FastAPI response object.
        pool_address (str): Pool address.
        block_number (Optional[int]): Block number (optional).

    Returns:
        dict: Trades snapshot or error message.
    """
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
        return {"error": str(e)}


@router.get('/snapshot/allTrades')
@router.get('/snapshot/allTrades/{block_number}')
@router.get('/mpp/snapshot/allTrades', tags=['uniswap', 'mpp'])
@router.get('/mpp/snapshot/allTrades/{block_number}', tags=['uniswap', 'mpp'])
async def get_all_trades_snapshot(
    request: Request,
    response: Response,
    block_number: Optional[int] = None,
):
    """
    Retrieve the trades snapshot for all pools, optionally at a specific block.

    Args:
        request (Request): FastAPI request object.
        response (Response): FastAPI response object.
        block_number (Optional[int]): Block number (optional).

    Returns:
        dict: Trades snapshot or error message.
    """
    import asyncio
    import time
    
    # Log immediately at function entry
    rest_logger.info(f"[allTrades] ENDPOINT HANDLER CALLED - block_number: {block_number}")
    
    request_start = time.time()
    rest_logger.info(
        f"[allTrades] Request received - block_number: {block_number or 'latest'}, "
        f"redis_conn: {request.app.state.redis_conn is not None}, "
        f"ipfs_reader: {request.app.state.ipfs_reader_client is not None}"
    )

    try:
        # Add timeout to prevent hanging on data retrieval
        rest_logger.info(f"[allTrades] Starting data fetch with 60s timeout...")
        fetch_start = time.time()
        trades_snapshot = await asyncio.wait_for(
            get_uniswap_v3_all_trades_snapshot(
                redis_conn=request.app.state.redis_conn,
                protocol_state_contract=request.app.state.protocol_state_contract,
                anchor_rpc_helper=request.app.state.anchor_rpc_helper,
                ipfs_reader=request.app.state.ipfs_reader_client,
                block_number=block_number,
            ),
            timeout=60.0  # 60 second timeout for the entire operation
        )
        fetch_duration = time.time() - fetch_start
        rest_logger.info(f"[allTrades] Data fetch completed in {fetch_duration:.2f}s")

        if not trades_snapshot:
            rest_logger.warning(
                f"AllTrades snapshot not found for block_number: {block_number or 'latest'}"
            )
            response.status_code = 404
            return {"error": "Trades snapshot not found"}
        else:
            # Count pools in the snapshot
            pool_count = len(trades_snapshot.get('tradeData', {})) if isinstance(trades_snapshot, dict) else 0
            rest_logger.info(
                f"Successfully fetched allTrades snapshot - block_number: {block_number or 'latest'}, "
                f"pools: {pool_count}"
            )
            response.status_code = 200
            return trades_snapshot
    except asyncio.TimeoutError:
        rest_logger.error(
            f"Timeout fetching allTrades snapshot for block_number: {block_number or 'latest'}"
        )
        response.status_code = 504  # Gateway Timeout
        return {"error": "Request timeout - data retrieval took too long"}
    except Exception as e:
        rest_logger.opt(exception=True).error(f"Error getting trades snapshot for all pools at block {block_number}: {e}")
        response.status_code = 500
        return {"error": str(e)}


@router.get('/mpp/stream/allTrades', tags=['uniswap', 'mpp', 'streaming'])
async def mpp_stream_all_trades(
    request: Request,
    from_epoch: Optional[int] = None,
):
    """
    Server-Sent Events stream of all-pool Uniswap V3 trades per finalized epoch (block).

    Event-driven via Redis pub/sub on ``SnapshotFinalized`` events published by
    ``unified_cache``.  Falls back to polling ``projectLastFinalizedEpoch`` Redis
    hashmap and ultimately the ``lastFinalizedSnapshot`` contract call.

    One MPP charge applies per HTTP connection (see MppConfig.stream_amount).
    Optional query param ``from_epoch`` sets the starting epoch; defaults to the
    latest finalized epoch for the allTrades project when omitted.

    If a finalized epoch has no snapshot (null CID, empty trades, failed submission),
    the server retries a few times then emits
    ``{"epoch": N, "skipped": true, "reason": "snapshot_unavailable"}`` and advances
    so the stream does not stall forever on a gap.
    """
    if not getattr(request.app.state, 'ipfs_reader_client', None):
        async def err_no_ipfs():
            yield f"data: {json.dumps({'error': 'IPFS not configured'})}\n\n"

        return StreamingResponse(err_no_ipfs(), media_type='text/event-stream')

    project_id = f"allTradesSnapshot:{settings.data_market}:{settings.namespace}"
    channel = snapshot_finalized_channel(project_id)
    pubsub_timeout_s = 15.0
    retry_sleep_s = 2.0

    async def _latest_finalized_epoch_from_redis(redis_conn) -> Optional[int]:
        raw = await redis_conn.hget(project_last_finalized_epoch_hmap(), project_id)
        return int(raw) if raw is not None else None

    async def _latest_finalized_epoch_from_contract() -> Optional[int]:
        try:
            ps = request.app.state.protocol_state_contract
            [epoch] = await request.app.state.anchor_rpc_helper.web3_call(
                tasks=[
                    ('lastFinalizedSnapshot', [Web3.to_checksum_address(settings.data_market), project_id]),
                ],
                contract_addr=ps.address,
                abi=ps.abi,
            )
            return int(epoch) if int(epoch) > 0 else None
        except Exception as e:
            rest_logger.warning(f'mpp_stream: lastFinalizedSnapshot contract call failed: {e}')
            return None

    async def _fetch_and_yield(epoch_id):
        """Fetch snapshot for a single epoch. Returns (payload_str, success)."""
        try:
            trades_snapshot = await asyncio.wait_for(
                get_uniswap_v3_all_trades_snapshot(
                    redis_conn=request.app.state.redis_conn,
                    protocol_state_contract=request.app.state.protocol_state_contract,
                    anchor_rpc_helper=request.app.state.anchor_rpc_helper,
                    ipfs_reader=request.app.state.ipfs_reader_client,
                    block_number=epoch_id,
                ),
                timeout=120.0,
            )
        except asyncio.TimeoutError:
            rest_logger.warning(f'mpp_stream: timeout fetching epoch {epoch_id}')
            return None, False
        except Exception as e:
            rest_logger.exception(f'mpp_stream: fetch error epoch {epoch_id}', e=e)
            return None, False

        if not trades_snapshot:
            return None, False

        payload = {
            'epoch': epoch_id,
            'snapshot': _snapshot_model_to_json(trades_snapshot),
        }
        return f"data: {json.dumps(payload, default=str)}\n\n", True

    async def event_stream():
        redis_conn = request.app.state.redis_conn
        pubsub = redis_conn.pubsub()

        next_epoch = from_epoch
        if next_epoch is None:
            next_epoch = await _latest_finalized_epoch_from_redis(redis_conn)
        if next_epoch is None:
            next_epoch = await _latest_finalized_epoch_from_contract()
        if next_epoch is None:
            try:
                next_epoch = await _anchor_current_epoch_id(request)
            except Exception as e:
                rest_logger.exception('mpp_stream: failed to determine starting epoch', e=e)
                yield f"data: {json.dumps({'error': f'cannot determine starting epoch: {e}'})}\n\n"
                return

        async def _drain_epoch_window(upper: int):
            """Advance next_epoch through upper, yielding SSE lines. Skips epochs with no snapshot after retries."""
            nonlocal next_epoch
            attempts = 0
            while next_epoch <= upper:
                sse_line, ok = await _fetch_and_yield(next_epoch)
                if ok:
                    yield sse_line
                    next_epoch += 1
                    attempts = 0
                    continue
                attempts += 1
                if attempts >= MPP_STREAM_SSE_FETCH_ATTEMPTS_PER_EPOCH:
                    rest_logger.warning(
                        f'mpp_stream: skipping epoch {next_epoch} after {attempts} failed fetches '
                        f'(no snapshot / null CID — empty block or missed submission)',
                    )
                    skip_payload = {
                        'epoch': next_epoch,
                        'skipped': True,
                        'reason': 'snapshot_unavailable',
                    }
                    yield f"data: {json.dumps(skip_payload)}\n\n"
                    next_epoch += 1
                    attempts = 0
                else:
                    await asyncio.sleep(retry_sleep_s)

        try:
            await pubsub.subscribe(channel)
            rest_logger.info(f'mpp_stream: subscribed to {channel}, starting at epoch {next_epoch}')

            while True:
                msg = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=pubsub_timeout_s,
                )

                if msg and msg['type'] == 'message':
                    try:
                        data = json.loads(msg['data'])
                        finalized_epoch = int(data['epochId'])
                    except (json.JSONDecodeError, KeyError, ValueError):
                        continue

                    if finalized_epoch < next_epoch:
                        rest_logger.debug(
                            f'mpp_stream: stale pub/sub epoch {finalized_epoch}, cursor at {next_epoch}, skipping',
                        )
                        continue

                    async for line in _drain_epoch_window(finalized_epoch):
                        yield line
                else:
                    latest = await _latest_finalized_epoch_from_redis(redis_conn)
                    if latest is None:
                        latest = await _latest_finalized_epoch_from_contract()
                    if latest is not None and latest < next_epoch:
                        rest_logger.debug(
                            f'mpp_stream: fallback latest {latest} behind cursor {next_epoch}, waiting',
                        )
                        continue
                    if latest is not None:
                        async for line in _drain_epoch_window(latest):
                            yield line
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.close()

    return StreamingResponse(event_stream(), media_type='text/event-stream')


@router.get('/tokenPrices/all/{token_address}')
@router.get('/tokenPrices/all/{token_address}/{block_number}')
async def get_token_price_all(
    request: Request,
    response: Response,
    token_address: str,
    block_number: Optional[int] = None,
):
    """
    Retrieve all price snapshots for a token, optionally at a specific block.

    Args:
        request (Request): FastAPI request object.
        response (Response): FastAPI response object.
        token_address (str): Token address.
        block_number (Optional[int]): Block number (optional).

    Returns:
        dict: Token price snapshots or error message.
    """
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
        return {"error": str(e)}


@router.get('/tradeVolumeAllPools/{token_address}/{time_interval}')
async def get_trade_volume_agg_all_pools(
    request: Request,
    response: Response,
    token_address: str,
    time_interval: int,
):  
    """
    Retrieve aggregated trade volume for all pools of a token over a given time interval.

    Args:
        request (Request): FastAPI request object.
        response (Response): FastAPI response object.
        token_address (str): Token address.
        time_interval (int): Time interval in seconds.

    Returns:
        dict: Trade volume aggregation or error message.
    """
    try:
        trade_volume_agg = await get_uniswap_trade_volume_agg_all_pools(
            redis_conn=request.app.state.redis_conn,
            anchor_rpc_helper=request.app.state.anchor_rpc_helper,
            ipfs_reader=request.app.state.ipfs_reader_client,
            protocol_state_contract=request.app.state.protocol_state_contract,
            time_interval=time_interval,
            token_address=token_address,
        )
        if not trade_volume_agg:
            response.status_code = 404
            return {"error": "Trade volume agg not found"}
        else:
            response.status_code = 200
            return trade_volume_agg
    except Exception as e:
        rest_logger.opt(exception=True).error(f"Error getting trade volume agg for {token_address}: {e}")
        response.status_code = 500
        return {"error": str(e)}


@router.get('/tradeVolume/{pool_address}/{time_interval}')
async def get_trade_volume_agg(
    request: Request,
    response: Response,
    pool_address: str,
    time_interval: int,
):
    """
    Retrieve aggregated trade volume for a specific pool over a given time interval.

    Args:
        request (Request): FastAPI request object.
        response (Response): FastAPI response object.
        pool_address (str): Pool address.
        time_interval (int): Time interval in seconds.

    Returns:
        dict: Trade volume aggregation or error message.
    """
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
        if not trade_volume_agg:
            response.status_code = 404
            return {"error": "Trade volume agg not found"}
        else:
            response.status_code = 200
            return trade_volume_agg
    except Exception as e:
        rest_logger.opt(exception=True).error(f"Error getting trade volume agg for {pool_address}: {e}")
        response.status_code = 500
        return {"error": str(e)}
    

@router.get('/poolTrades/{pool_address}/{start_timestamp}/{end_timestamp}')
async def get_pool_trades(
    request: Request,
    response: Response,
    pool_address: str,
    start_timestamp: int,
    end_timestamp: int,
):
    """
    Retrieve all trades for a pool between two timestamps.

    Args:
        request (Request): FastAPI request object.
        response (Response): FastAPI response object.
        pool_address (str): Pool address.
        start_timestamp (int): Start timestamp (unix seconds).
        end_timestamp (int): End timestamp (unix seconds).

    Returns:
        dict: List of trades or error message.
    """
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

        if not pool_trades:
            response.status_code = 404
            return {"error": "Pool trades not found"}
        else:
            response.status_code = 200
            return pool_trades

    except Exception as e:
        rest_logger.opt(exception=True).error(f"Error getting pool trades for {pool_address}: {e}")
        response.status_code = 500
        return {"error": str(e)}
    


@router.get('/timeSeries/{token_address}/{pool_address}/{time_interval}/{step_seconds}')
async def get_token_price_series(
    request: Request,
    response: Response,
    token_address: str,
    pool_address: str,
    time_interval: int,
    step_seconds: int,
):
    """
    Retrieve a time series of token prices for a given pool and token.

    Args:
        request (Request): FastAPI request object.
        response (Response): FastAPI response object.
        token_address (str): Token address.
        pool_address (str): Pool address.
        time_interval (int): Time interval in seconds.
        step_seconds (int): Step size in seconds.

    Returns:
        dict: Token price time series or error message.
    """
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
        return {"error": str(e)}


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

    Args:
        request (Request): FastAPI request object.
        response (Response): FastAPI response object.
        page (int): Page number to retrieve (starts at 1).
        size (int): Number of items per page (default: 50, max: 100).
        metadata (bool): Include token metadata in the response (default: False).
        time_interval (int): Time interval in seconds (default: 86400).

    Returns:
        dict: List of active tokens with their frequencies and optional metadata,
              plus pagination metadata.
    """
    rest_logger.info(
        f"Fetching daily active tokens - page: {page}, size: {size}, "
        f"time_interval: {time_interval}s, metadata: {metadata}"
    )
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
        
        rest_logger.info(
            f"Successfully fetched daily active tokens - total: {total_tokens}, "
            f"returned: {len(tokens_data)}, page: {page}/{((total_tokens + size - 1) // size)}"
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
        return {"error": str(e)}
    

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

    Args:
        request (Request): FastAPI request object.
        response (Response): FastAPI response object.
        page (int): Page number to retrieve (starts at 1).
        size (int): Number of items per page (default: 50, max: 100).
        metadata (bool): Include pool metadata in the response (default: False).
        time_interval (int): Time interval in seconds (default: 86400).

    Returns:
        dict: List of active pools with their frequencies and optional metadata,
              plus pagination metadata.
    """
    rest_logger.info(
        f"Fetching daily active pools - page: {page}, size: {size}, "
        f"time_interval: {time_interval}s, metadata: {metadata}"
    )
    try:
        pools_data, total_pools = await get_active_pools(
            redis_conn=request.app.state.redis_conn,
            protocol_state_contract=request.app.state.protocol_state_contract,
            rpc_helper=request.app.state.rpc_helper,
            anchor_rpc_helper=request.app.state.anchor_rpc_helper,
            ipfs_reader=request.app.state.ipfs_reader_client,
            time_interval=time_interval,
            page=page,
            size=size,
            metadata=metadata,
        )
    
        rest_logger.info(
            f"Successfully fetched daily active pools - total: {total_pools}, "
            f"returned: {len(pools_data)}, page: {page}/{((total_pools + size - 1) // size)}"
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
        return {"error": str(e)}
