"""Hibachi SDK 래퍼 — REST + WS 마켓 데이터"""
from __future__ import annotations

import os
import logging
import asyncio
from typing import Callable

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 2


def _setup_env(api_key: str, private_key: str, public_key: str, account_id: str):
    os.environ["HIBACHI_API_ENDPOINT_PRODUCTION"] = "https://api.hibachi.xyz"
    os.environ["HIBACHI_DATA_API_ENDPOINT_PRODUCTION"] = "https://data-api.hibachi.xyz"
    os.environ["HIBACHI_API_KEY_PRODUCTION"] = api_key
    os.environ["HIBACHI_PRIVATE_KEY_PRODUCTION"] = private_key
    os.environ["HIBACHI_PUBLIC_KEY_PRODUCTION"] = public_key
    os.environ["HIBACHI_ACCOUNT_ID_PRODUCTION"] = account_id


async def _retry(fn, *args, **kwargs):
    """I5: SDK 호출 재시도 래퍼"""
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            return await fn(*args, **kwargs)
        except Exception as e:
            last_err = e
            logger.warning("Hibachi API retry %d/%d: %s", attempt + 1, MAX_RETRIES, e)
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
    raise last_err


class HibachiClient:
    def __init__(self, api_key: str, private_key: str, public_key: str, account_id: str):
        _setup_env(api_key, private_key, public_key, account_id)
        self._rest = None
        self._initialized = False

    def _ensure_sdk(self):
        if not self._initialized:
            from hibachi_xyz import HibachiApiClient
            self._rest = HibachiApiClient()
            # SDK에 명시적으로 credentials 설정
            self._rest.set_api_key(os.environ.get("HIBACHI_API_KEY_PRODUCTION", ""))
            self._rest.set_private_key(os.environ.get("HIBACHI_PRIVATE_KEY_PRODUCTION", ""))
            self._rest.set_account_id(os.environ.get("HIBACHI_ACCOUNT_ID_PRODUCTION", ""))
            self._initialized = True

    # --- 내부 raw 호출 (SDK 객체 반환) ---

    async def _get_balance_raw(self):
        self._ensure_sdk()
        return await asyncio.to_thread(self._rest.get_capital_balance)

    async def _get_account_info_raw(self):
        self._ensure_sdk()
        return await asyncio.to_thread(self._rest.get_account_info)

    async def _get_prices_raw(self, symbol: str):
        self._ensure_sdk()
        return await asyncio.to_thread(self._rest.get_prices, symbol)

    async def _get_stats_raw(self, symbol: str):
        self._ensure_sdk()
        return await asyncio.to_thread(self._rest.get_stats, symbol)

    async def _place_order_raw(self, symbol: str, side: str, price: float,
                               quantity: float, post_only: bool = False):
        self._ensure_sdk()
        from hibachi_xyz import Side, OrderFlags
        sdk_side = Side.BUY if side.upper() == "BUY" else Side.SELL
        flags = OrderFlags.PostOnly if post_only else None
        return await asyncio.to_thread(
            self._rest.place_limit_order,
            symbol=symbol, quantity=quantity, price=price,
            side=sdk_side, max_fees_percent=0.1,
            order_flags=flags,
        )

    async def _cancel_order_raw(self, order_id: str):
        self._ensure_sdk()
        return await asyncio.to_thread(self._rest.cancel_order, order_id=order_id)

    @staticmethod
    def _to_dict(obj) -> dict:
        """SDK dataclass/tuple/dict → dict 안전 변환"""
        if isinstance(obj, dict):
            return obj
        if hasattr(obj, "__dataclass_fields__"):
            try:
                from dataclasses import asdict
                return asdict(obj)
            except TypeError:
                pass
        if hasattr(obj, "__dict__"):
            return obj.__dict__
        return {"raw": str(obj)}

    # --- 공개 인터페이스 (dict/float 반환) ---

    async def get_balance(self) -> dict:
        result = await _retry(self._get_balance_raw)
        # SDK dataclass → dict 변환
        if not isinstance(result, dict):
            return self._to_dict(result)
        if isinstance(result, dict):
            return result
        return {"raw": str(result)}

    async def get_positions(self) -> list:
        result = await _retry(self._get_account_info_raw)
        positions = []
        raw_positions = getattr(result, "positions", []) if hasattr(result, "positions") else []
        for p in raw_positions:
            positions.append(self._to_dict(p) if not isinstance(p, dict) else p)
        return positions

    async def get_mark_price(self, symbol: str) -> float:
        result = await _retry(self._get_prices_raw, symbol)
        # SDK PriceResponse: markPrice 속성
        if hasattr(result, "markPrice"):
            return float(result.markPrice)
        if isinstance(result, dict):
            return float(result.get("markPrice", result.get("mark_price", 0)))
        return 0.0

    async def get_bbo(self, symbol: str) -> dict:
        result = await _retry(self._get_prices_raw, symbol)
        bid = ask = mark = 0.0
        if hasattr(result, "markPrice"):
            mark = float(result.markPrice)
            bid = float(getattr(result, "bidPrice", 0) or 0)
            ask = float(getattr(result, "askPrice", 0) or 0)
        elif isinstance(result, dict):
            mark = float(result.get("markPrice", result.get("mark_price", 0)))
            bid = float(result.get("bidPrice", result.get("bid_price", 0)))
            ask = float(result.get("askPrice", result.get("ask_price", 0)))
        return {"bid": bid, "ask": ask, "mark": mark}

    async def get_funding_rate(self, symbol: str) -> float:
        # 펀딩레이트는 get_prices의 fundingRateEstimation에 있음 (get_stats에는 없음!)
        result = await _retry(self._get_prices_raw, symbol)
        if hasattr(result, "fundingRateEstimation"):
            est = result.fundingRateEstimation
            if hasattr(est, "estimatedFundingRate") and est.estimatedFundingRate:
                return float(est.estimatedFundingRate)
        # fallback: get_stats 시도
        result = await _retry(self._get_stats_raw, symbol)
        if hasattr(result, "fundingRate"):
            return float(result.fundingRate)
        if isinstance(result, dict):
            return float(result.get("fundingRate", result.get("funding_rate", 0)))
        return 0.0

    async def place_limit_order(self, symbol: str, side: str, price: float, size: float,
                                post_only: bool = False) -> dict:
        result = await _retry(
            self._place_order_raw,
            symbol=symbol, side=side, price=price, quantity=size, post_only=post_only,
        )
        if not isinstance(result, dict):
            return self._to_dict(result)
        if isinstance(result, dict):
            return result
        return {"raw": str(result)}

    async def cancel_order(self, order_id: str) -> dict:
        result = await _retry(self._cancel_order_raw, order_id)
        if not isinstance(result, dict):
            return self._to_dict(result)
        return {"raw": str(result)}

    async def _cancel_all_raw(self):
        self._ensure_sdk()
        return await asyncio.to_thread(self._rest.cancel_all_orders)

    async def cancel_all_orders(self) -> dict:
        result = await _retry(self._cancel_all_raw)
        return {"raw": str(result)}

    async def close_position(self, symbol: str, side: str, size: float,
                             slippage_pct: float = 0.005) -> dict:
        close_side = "SELL" if side == "BUY" else "BUY"
        price = await self.get_mark_price(symbol)
        if close_side == "BUY":
            price *= (1 + slippage_pct)
        else:
            price *= (1 - slippage_pct)
        return await self.place_limit_order(symbol, close_side, round(price, 2), size)

    async def close(self):
        pass


class HibachiWSClient:
    def __init__(self, ws_url: str, symbol: str, on_price: Callable[[float], None]):
        self.ws_url = ws_url
        self.symbol = symbol
        self.on_price = on_price
        self._running = False
        self._ws_instance = None

    async def connect(self):
        self._running = True
        retry_delay = 1
        while self._running:
            try:
                from hibachi_xyz import HibachiWSMarketClient
                ws = HibachiWSMarketClient()
                self._ws_instance = ws

                def _on_mark_price(data):
                    try:
                        sym = getattr(data, "symbol", None) or (data.get("symbol") if isinstance(data, dict) else None)
                        if sym == self.symbol:
                            mark = float(getattr(data, "markPrice", None) or
                                         (data.get("markPrice", data.get("mark_price", 0)) if isinstance(data, dict) else 0))
                            if mark > 0:
                                self.on_price(mark)
                    except (ValueError, KeyError, TypeError):
                        pass

                from hibachi_xyz import WebSocketSubscription, WebSocketSubscriptionTopic
                ws.on("mark_price", _on_mark_price)
                logger.info("Hibachi WS connecting: %s", self.symbol)
                retry_delay = 1
                # connect → subscribe → 블로킹 대기
                await ws.connect()
                await ws.subscribe([WebSocketSubscription(
                    symbol=self.symbol,
                    topic=WebSocketSubscriptionTopic.MARK_PRICE,
                )])
                logger.info("Hibachi WS subscribed: %s", self.symbol)
                # WS는 이벤트 콜백으로 데이터 수신, 여기서 대기
                while self._running:
                    await asyncio.sleep(5)
            except Exception as e:
                if not self._running:
                    break
                logger.warning("Hibachi WS disconnected, retry in %ds: %s", retry_delay, e)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)

    async def disconnect(self):
        self._running = False
        if self._ws_instance is not None:
            try:
                await self._ws_instance.disconnect()
            except Exception:
                pass
            self._ws_instance = None
