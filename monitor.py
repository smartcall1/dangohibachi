"""마진/Circuit Breaker 모니터링"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from config import Config

logger = logging.getLogger(__name__)


class MarginMonitor:
    """
    Hibachi 마진 비율을 주기적으로 감시하고 콜백 트리거.
    엔진이 asyncio.create_task(monitor.run())으로 백그라운드 실행.
    """

    def __init__(self, hibachi_client, on_emergency: callable):
        self._hb = hibachi_client
        self._on_emergency = on_emergency
        self._running = False
        self._margin_pct: float = 100.0

    @property
    def margin_pct(self) -> float:
        return self._margin_pct

    async def run(self):
        self._running = True
        while self._running:
            try:
                await self._check()
            except Exception as e:
                logger.warning("MarginMonitor 오류: %s", e)
            await asyncio.sleep(10)

    async def _check(self):
        try:
            balance = await self._hb.get_balance()
            positions = await self._hb.get_positions()
        except Exception as e:
            logger.warning("Margin check API 실패 (이전 margin_pct=%.1f%% 유지): %s",
                           self._margin_pct, e)
            return

        if positions is None:
            logger.warning("Hibachi positions=None — margin check 스킵")
            return

        equity = float(balance.get("balance", balance.get("equity", balance.get("totalEquityValue", 0))) or 0)
        if not positions or equity <= 0:
            logger.warning("MarginMonitor early exit: positions=%d equity=%.2f bal_keys=%s",
                           len(positions) if positions else 0, equity, list(balance.keys()))
            self._margin_pct = 100.0
            return

        total_notional = 0.0
        for p in positions:
            qty = abs(float(p.get("quantity", p.get("size", p.get("position_size", 0))) or 0))
            symbol = p.get("symbol", "")
            if qty <= 0 or not symbol:
                logger.warning("MarginMonitor position skip: qty=%.8f symbol=%r keys=%s",
                               qty, symbol, list(p.keys()))
                continue
            try:
                mark = await self._hb.get_mark_price(symbol)
                total_notional += qty * mark
            except Exception as e:
                logger.warning("MarginMonitor mark_price 실패: symbol=%s err=%s", symbol, e)

        if total_notional > 0:
            self._margin_pct = (equity / total_notional) * 100
        else:
            logger.warning("MarginMonitor total_notional=0 (positions=%d) — 100%% fallback. "
                           "first_pos_keys=%s", len(positions), list(positions[0].keys()) if positions else [])
            self._margin_pct = 100.0

        if self._margin_pct <= Config.MARGIN_WARNING_PCT:
            logger.warning("Hibachi 마진 경고: %.1f%%", self._margin_pct)

        if self._margin_pct <= Config.MARGIN_EMERGENCY_PCT:
            logger.error("Hibachi 마진 긴급: %.1f%% → 긴급 청산 트리거", self._margin_pct)
            await self._on_emergency(self._margin_pct)

    async def stop(self):
        self._running = False


class DangoHealthMonitor:
    """
    Dango API 헬스 체크. 장애 감지 시 on_down / 복구 시 on_up 콜백.
    """

    def __init__(self, dango_client, on_down: callable, on_up: callable):
        self._dango = dango_client
        self._on_down = on_down
        self._on_up = on_up
        self._is_down = False
        self._running = False

    async def run(self):
        self._running = True
        while self._running:
            try:
                healthy = await self._dango.is_healthy()
                if not healthy and not self._is_down:
                    self._is_down = True
                    logger.error("Dango API 다운 감지")
                    await self._on_down()
                elif healthy and self._is_down:
                    self._is_down = False
                    logger.info("Dango API 복구")
                    await self._on_up()
            except Exception as e:
                logger.warning("DangoHealthMonitor 오류: %s", e)
            await asyncio.sleep(15)

    @property
    def is_down(self) -> bool:
        return self._is_down

    async def stop(self):
        self._running = False
