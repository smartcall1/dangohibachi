"""텔레그램 알림 + 전역 키보드 UI (aiohttp 폴링)"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Awaitable, Callable, Optional

import aiohttp

logger = logging.getLogger(__name__)

# 하단 고정 버튼 텍스트
BTN_STATUS = "📊 Status"
BTN_FUNDING = "💰 Funding"
BTN_HISTORY = "📋 History"
BTN_POSITIONS = "📌 Positions"
BTN_RESYNC = "🔄 Resync"
BTN_CLOSE = "🔚 Close Now"
BTN_STOP = "⏹ Stop"

KEYBOARD = {
    "keyboard": [
        [BTN_STATUS, BTN_FUNDING],
        [BTN_HISTORY, BTN_POSITIONS],
        [BTN_RESYNC, BTN_CLOSE],
        [BTN_STOP],
    ],
    "resize_keyboard": True,
    "is_persistent": True,
}


class TelegramUI:
    def __init__(self, token: str, chat_id: str):
        self._token = token
        self._chat_id = chat_id
        self._base = f"https://api.telegram.org/bot{token}" if token else ""
        self._session: Optional[aiohttp.ClientSession] = None
        self._offset = 0
        self._callbacks: dict[str, Callable[..., Awaitable]] = {}
        self._engine_ref = None
        self.enabled = bool(token and chat_id)

    def set_engine(self, engine):
        # 호환용 — 엔진이 register_callback으로 직접 등록하므로 ref만 보관
        self._engine_ref = engine

    def register_callback(self, button: str, handler: Callable[..., Awaitable]):
        self._callbacks[button] = handler

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()

    async def start(self):
        if not self.enabled:
            logger.warning("TELEGRAM_BOT_TOKEN/CHAT_ID 미설정 — 텔레그램 비활성")
            return
        await self._ensure_session()
        logger.info("텔레그램 봇 시작 (aiohttp polling + persistent keyboard)")

    async def stop(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ──────────────────────────────────────────────
    # 메시지 전송
    # ──────────────────────────────────────────────

    async def send_message(self, text: str, with_keyboard: bool = True):
        if not self.enabled:
            return
        await self._ensure_session()
        payload = {"chat_id": self._chat_id, "text": text, "parse_mode": "HTML"}
        if with_keyboard:
            payload["reply_markup"] = json.dumps(KEYBOARD)
        try:
            async with self._session.post(
                f"{self._base}/sendMessage", json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    logger.warning("텔레그램 전송 실패: %d", resp.status)
        except Exception as e:
            logger.warning("텔레그램 전송 오류: %s", e)

    async def send(self, text: str):
        """legacy alias — 키보드 함께 전송"""
        await self.send_message(text, with_keyboard=True)

    async def send_alert(self, text: str):
        """알림 전용 — 키보드 함께 (persistent라 한 번만 떠도 유지)"""
        await self.send_message(text, with_keyboard=True)

    # ──────────────────────────────────────────────
    # 버튼 폴링
    # ──────────────────────────────────────────────

    async def poll_updates(self):
        if not self.enabled:
            return
        await self._ensure_session()
        try:
            async with self._session.get(
                f"{self._base}/getUpdates",
                params={"offset": self._offset, "timeout": 2},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
                for update in data.get("result", []):
                    self._offset = update["update_id"] + 1
                    msg = update.get("message", {})
                    if str(msg.get("chat", {}).get("id")) != str(self._chat_id):
                        continue
                    text = (msg.get("text") or "").strip()
                    handler = self._callbacks.get(text)
                    if handler:
                        try:
                            await handler()
                        except Exception as e:
                            logger.error("텔레그램 핸들러 예외 (%s): %s", text, e, exc_info=True)
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            logger.debug("텔레그램 폴링 오류: %s", e)

    # ──────────────────────────────────────────────
    # 이벤트 알림 메서드 (engine에서 호출)
    # ──────────────────────────────────────────────

    async def notify_enter(
        self, pair: str, direction: str, notional: float,
        price: float, cycle: int, chunks: int = 0, total_chunks: int = 0,
        dango_size: float = 0.0, hibachi_size: float = 0.0,
    ):
        await self.send_alert(
            f"✅ <b>[ENTER #{cycle}]</b> {pair} {direction}\n"
            f"Notional: ${notional:,.0f} | 진입가: {price:.2f}\n"
            f"청크: {chunks}/{total_chunks}\n"
            f"Dango: {dango_size:.6f} | Hibachi: {hibachi_size:.6f}"
        )

    async def notify_exit(
        self, pair: str, direction: str, reason: str,
        pnl: float, cycle: int, entry_price: float = 0.0,
        chunks: int = 0, total_chunks: int = 0, balance: float = 0.0,
    ):
        emoji = "🟢" if pnl >= 0 else "🔴"
        await self.send_alert(
            f"{emoji} <b>[EXIT #{cycle}]</b> {pair} {direction}\n"
            f"사유: {reason}\n"
            f"진입가: {entry_price:.2f} | 청크: {chunks}/{total_chunks}\n"
            f"PnL: ${pnl:+.2f} | 잔고: ${balance:,.2f}"
        )

    async def notify_margin_warning(self, exchange: str, pct: float):
        await self.send_alert(f"⚠️ <b>[MARGIN WARNING]</b> {exchange} {pct:.1f}%")

    async def notify_dango_down(self):
        await self.send_alert("🔧 <b>[DANGO DOWN]</b> HOLD_SUSPENDED 진입 — API 복구 대기 중")

    async def notify_dango_up(self):
        await self.send_alert("✅ <b>[DANGO UP]</b> API 복구 — HOLD 재개")

    async def notify_hibachi_closed_only(self, size: float):
        await self.send_alert(
            f"🚨 <b>[HIBACHI CLOSED]</b> Dango 다운 중 긴급 Hibachi 청산!\n"
            f"Dango 포지션 수동 청산 필요: size={size:.6f}"
        )

    async def notify_manual_intervention(
        self, failures: int, cycle: int, pair: str = "?",
        dango_size: float = 0.0, dango_notional: float = 0.0,
        hibachi_size: float = 0.0, hibachi_notional: float = 0.0,
    ):
        lines = [
            f"🚨 <b>[MANUAL]</b> {pair} | EXIT 실패 {failures}회 — 수동 개입 필요",
            f"사이클: #{cycle}",
            f"Dango:   {dango_size:+.6f} (${dango_notional:,.2f})",
            f"Hibachi: {hibachi_size:+.6f} (${hibachi_notional:,.2f})",
        ]
        await self.send_alert("\n".join(lines))

    async def notify_chunk_filled(self, chunk_idx: int, total: int, price: float, size: float):
        await self.send_alert(
            f"⚡ <b>[CHUNK {chunk_idx}/{total}]</b> 체결 {price:.2f} × {size:.6f}"
        )
