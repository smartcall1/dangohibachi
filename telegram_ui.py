"""텔레그램 알림 및 커맨드 핸들러"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class TelegramUI:
    def __init__(self, bot_token: str, chat_id: str):
        self._token = bot_token
        self._chat_id = chat_id
        self._app = None
        self._engine_ref = None

    def set_engine(self, engine):
        self._engine_ref = engine

    async def start(self):
        if not self._token:
            logger.warning("TELEGRAM_BOT_TOKEN 미설정 — 텔레그램 비활성")
            return
        try:
            from telegram import Update
            from telegram.ext import Application, CommandHandler, ContextTypes

            self._app = Application.builder().token(self._token).build()
            self._app.add_handler(CommandHandler("status", self._cmd_status))
            self._app.add_handler(CommandHandler("stop", self._cmd_stop))
            await self._app.initialize()
            await self._app.start()
            await self._app.updater.start_polling(drop_pending_updates=True)
            logger.info("텔레그램 봇 시작")
        except Exception as e:
            logger.warning("텔레그램 시작 실패: %s", e)

    async def stop(self):
        if self._app:
            try:
                await self._app.updater.stop()
                await self._app.stop()
                await self._app.shutdown()
            except Exception:
                pass

    async def send(self, text: str):
        if not self._token:
            return
        try:
            import httpx
            url = f"https://api.telegram.org/bot{self._token}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={"chat_id": self._chat_id, "text": text, "parse_mode": "HTML"})
        except Exception as e:
            logger.warning("텔레그램 전송 실패: %s", e)

    # ──────────────────────────────────────────────
    # 이벤트별 알림 메서드
    # ──────────────────────────────────────────────

    async def notify_enter(self, pair: str, direction: str, notional: float, price: float, cycle: int):
        await self.send(
            f"✅ <b>[ENTER #{cycle}]</b> {pair} {direction}\n"
            f"Notional: ${notional:,.0f} | 진입가: {price:.2f}"
        )

    async def notify_exit(self, reason: str, pnl: float, cycle: int):
        emoji = "🟢" if pnl >= 0 else "🔴"
        await self.send(
            f"{emoji} <b>[EXIT #{cycle}]</b> {reason}\n"
            f"PnL: ${pnl:+.2f}"
        )

    async def notify_margin_warning(self, exchange: str, pct: float):
        await self.send(f"⚠️ <b>[MARGIN WARNING]</b> {exchange} {pct:.1f}%")

    async def notify_dango_down(self):
        await self.send("🔧 <b>[DANGO DOWN]</b> HOLD_SUSPENDED 진입 — API 복구 대기 중")

    async def notify_dango_up(self):
        await self.send("✅ <b>[DANGO UP]</b> API 복구 — HOLD 재개")

    async def notify_hibachi_closed_only(self, size: float):
        await self.send(
            f"🚨 <b>[HIBACHI CLOSED]</b> Dango 다운 중 긴급 Hibachi 청산!\n"
            f"Dango 포지션 수동 청산 필요: size={size:.6f}"
        )

    async def notify_manual_intervention(self, failures: int, cycle: int):
        await self.send(
            f"🚨 <b>[MANUAL INTERVENTION]</b> EXIT 실패 {failures}회 — 수동 개입 필요\n"
            f"사이클: #{cycle}"
        )

    async def notify_chunk_filled(self, chunk_idx: int, total: int, price: float, size: float):
        await self.send(
            f"⚡ <b>[CHUNK {chunk_idx}/{total}]</b> 체결 {price:.2f} × {size:.6f}"
        )

    # ──────────────────────────────────────────────
    # 커맨드 핸들러
    # ──────────────────────────────────────────────

    async def _cmd_status(self, update, context):
        if not self._engine_ref:
            await update.message.reply_text("엔진 연결 안됨")
            return
        try:
            state = self._engine_ref.bot_state
            pos = state.position
            lines = [
                f"<b>상태:</b> {state.state.value}",
                f"<b>사이클:</b> #{state.cycle_count}",
            ]
            if pos:
                lines += [
                    f"<b>페어:</b> {pos.pair} {pos.direction.value}",
                    f"<b>보유:</b> {pos.hold_minutes:.0f}분",
                    f"<b>Dango 포지션:</b> {pos.dango_size:.6f}",
                    f"<b>Hibachi 포지션:</b> {pos.hibachi_size:.6f}",
                ]
            await update.message.reply_text("\n".join(lines), parse_mode="HTML")
        except Exception as e:
            await update.message.reply_text(f"오류: {e}")

    async def _cmd_stop(self, update, context):
        await update.message.reply_text("⏹ 봇 정지 요청 수신")
        if self._engine_ref:
            asyncio.create_task(self._engine_ref.request_stop())
