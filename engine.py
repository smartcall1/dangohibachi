"""상태머신 엔진 — XEMM 진입/보유/청산 루프"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
from typing import Optional

from config import Config
from models import BotState, Cycle, Direction, Position, State
from monitor import DangoHealthMonitor, MarginMonitor
from strategy import (
    calc_chunk_size,
    calc_entry_notional,
    select_pair_and_direction,
    should_exit,
)
from telegram_ui import TelegramUI

logger = logging.getLogger(__name__)


def _quantize_to_tick(price: float, tick: float) -> float:
    """가격을 tick 배수로 양자화. float 부동소수점 오차까지 제거.
    예: 78714.40000000001 → 78714.4 (tick=0.1)
    'Price X is not a multiple of tick size' 거부 회귀 방지."""
    if tick <= 0:
        return price
    decimals = max(0, -int(math.floor(math.log10(tick))))
    return round(round(price / tick) * tick, decimals)


class Engine:
    def __init__(self, dango, hibachi, tg: TelegramUI):
        self._dango = dango
        self._hb = hibachi
        self._tg = tg
        self.bot_state = BotState()
        self._stop_requested = False

        # 마진 모니터 (백그라운드)
        self._margin_monitor = MarginMonitor(hibachi, self._on_margin_emergency)
        self._health_monitor = DangoHealthMonitor(dango, self._on_dango_down, self._on_dango_up)

        self._state_path = os.path.join(Config.LOG_DIR, "bot_state.json")
        self._cycles_path = os.path.join(Config.LOG_DIR, "cycles.jsonl")

    # ──────────────────────────────────────────────
    # 진입점
    # ──────────────────────────────────────────────

    async def run(self):
        Config.ensure_dirs()
        await self._dango.start()
        asyncio.create_task(self._margin_monitor.run())
        asyncio.create_task(self._health_monitor.run())
        self._register_telegram_callbacks()
        logger.info("엔진 시작")
        await self._recovery_check()

        while not self._stop_requested:
            try:
                await self._tick()
                await self._tg.poll_updates()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("엔진 루프 예외: %s", e, exc_info=True)
                await asyncio.sleep(5)

        await self._shutdown()

    async def request_stop(self):
        self._stop_requested = True

    # ──────────────────────────────────────────────
    # 텔레그램 버튼 핸들러
    # ──────────────────────────────────────────────

    def _register_telegram_callbacks(self):
        from telegram_ui import (
            BTN_STATUS, BTN_FUNDING, BTN_HISTORY,
            BTN_POSITIONS, BTN_RESYNC, BTN_CLOSE, BTN_STOP,
        )
        self._tg.register_callback(BTN_STATUS, self._on_status)
        self._tg.register_callback(BTN_FUNDING, self._on_funding)
        self._tg.register_callback(BTN_HISTORY, self._on_history)
        self._tg.register_callback(BTN_POSITIONS, self._on_positions)
        self._tg.register_callback(BTN_RESYNC, self._on_resync)
        self._tg.register_callback(BTN_CLOSE, self._on_close_now)
        self._tg.register_callback(BTN_STOP, self._on_stop)

    async def _on_status(self):
        s = self.bot_state
        pos = s.position
        lines = []

        # ── 헤더 ──
        if pos and s.state == State.HOLD:
            hold_h = pos.hold_minutes / 60
            remain_d = max(0, Config.MAX_HOLD_DAYS - pos.hold_days)
            badge = "✅" if pos.hold_minutes >= Config.MIN_HOLD_MINUTES else f"⏰{Config.MIN_HOLD_MINUTES - pos.hold_minutes:.0f}m남음"
            lines.append(f"📊 #{s.cycle_count} HOLD · {pos.pair} {pos.direction.value} · 보유 {hold_h:.1f}h {badge}")
        else:
            lines.append(f"📊 {s.state.value}")
        lines.append("🎯 XEMM")
        lines.append("━━━━━━━━━━━━━━━━")

        # ── 잔고 + PnL ──
        try:
            d_bal = await self._dango.get_balance()
            d_equity = d_bal["equity"]
            d_avail = d_bal["available_margin"]
        except Exception:
            d_equity = d_avail = 0

        try:
            h_bal = await self._hb.get_balance()
            h_equity = float(h_bal.get("balance", h_bal.get("equity", 0)) or 0)
        except Exception:
            h_equity = 0

        total = d_equity + h_equity
        if pos:
            entry_total = pos.entry_total_balance if pos.entry_total_balance > 0 else pos.entry_balance
            pnl = total - entry_total if entry_total > 0 else 0
            pct = pnl / entry_total * 100 if entry_total > 0 else 0
            emoji = "🟢" if pnl >= 0 else "🔴"
            lines.append(f"💰 <b>${total:,.0f}</b> {emoji} <b>${pnl:+,.2f}</b> ({pct:+.2f}%)")
            lines.append(f"   (D ${d_equity:,.2f} / H ${h_equity:,.2f})")
            lines.append(f"   진입 ${entry_total:,.0f}")

            # 익절 트리거 (Dango 잔고 기준 — should_exit 로직과 동일)
            required = pos.entry_balance + pos.target_notional * Config.ROUND_TRIP_FEE_RATE + Config.PRINCIPAL_BUFFER_USD
            gap = required - d_equity
            if gap <= 0:
                lines.append("   🎯 원금회수 청산 임박!")
            else:
                lines.append(f"   🎯 원금회수까지 <b>${gap:,.2f}</b>")
        else:
            lines.append(f"💰 <b>${total:,.0f}</b>")
            lines.append(f"   (D ${d_equity:,.2f} / H ${h_equity:,.2f})")

        # ── 포지션 ──
        if pos:
            try:
                mark = await self._dango.get_mark_price(Config.DANGO_SYMBOL_MAP[pos.pair])
                d_chg = (mark - pos.avg_entry_price) / pos.avg_entry_price * 100 if pos.avg_entry_price > 0 else 0
                if pos.direction == Direction.B:
                    d_chg = -d_chg
            except Exception:
                mark = d_chg = 0

            imbalance = abs(pos.dango_size - pos.hibachi_size) / max(pos.dango_size, 1e-9) * 100
            delta_emoji = "✅" if imbalance <= 5 else "⚠️"
            lines.append("")
            lines.append(f"📍 헷지 {delta_emoji}")
            lines.append(f"   D {pos.dango_side:4} {pos.dango_size:.6f}  {d_chg:+.2f}%")
            lines.append(f"   H {pos.hibachi_side:4} {pos.hibachi_size:.6f}")
            lines.append(f"   청크 {pos.chunks_filled}/{Config.ENTRY_CHUNKS}")

            # ── 펀딩 ──
            try:
                d_fr = await self._dango.get_funding_rate(Config.DANGO_SYMBOL_MAP[pos.pair])
                h_fr = await self._hb.get_funding_rate(Config.HIBACHI_SYMBOL_MAP[pos.pair])
                if pos.direction == Direction.A:
                    net_8h = d_fr - h_fr
                else:
                    net_8h = h_fr - d_fr
                apr = net_8h * (365 * 24 / 8) * 100
                apr_emoji = "🚀" if apr >= 30 else "✅" if apr >= 10 else "⚠️" if apr >= 0 else "🔻"
                lines.append("")
                lines.append(f"📈 펀딩 APR {apr_emoji} <b>{apr:+.1f}%</b>")
                lines.append(f"   8h D {d_fr:+.6f} / H {h_fr:+.6f}")
            except Exception:
                pass

            # ── 만기 ──
            remain_d = max(0, Config.MAX_HOLD_DAYS - pos.hold_days)
            lines.append(f"⏳ 자동만기까지 {remain_d:.1f}일")

            if pos.exit_reason:
                lines.append(f"🔚 EXIT 사유: {pos.exit_reason}")
        else:
            lines.append("")
            lines.append("<i>(포지션 없음)</i>")

        # ── 누적 realized ──
        agg = self._aggregate_realized()
        lines.append("")
        lines.append("━ 누적 (realized) ━")
        if agg["count"] > 0:
            emoji = "🟢" if agg["pnl"] >= 0 else "🔴"
            lines.append(f"{emoji} {agg['count']}건 누적 ${agg['pnl']:+,.2f}")
        else:
            lines.append("🌱 아직 완료된 사이클 없음")

        await self._tg.send_alert("\n".join(lines))

    async def _on_funding(self):
        text = "💰 <b>펀딩레이트 (8h)</b>\n\n"
        for pair in Config.PAIRS:
            try:
                d_fr = await self._dango.get_funding_rate(Config.DANGO_SYMBOL_MAP[pair])
                h_fr = await self._hb.get_funding_rate(Config.HIBACHI_SYMBOL_MAP[pair])
                net_a = h_fr - d_fr  # Direction A: Dango LONG, Hibachi SHORT
                text += (
                    f"<b>{pair}</b>: dango={d_fr:+.5f}  hibachi={h_fr:+.5f}\n"
                    f"  net(A)={net_a:+.5f}  net(B)={-net_a:+.5f}\n"
                )
            except Exception as e:
                text += f"<b>{pair}</b>: 조회 실패 ({e})\n"
        await self._tg.send_alert(text)

    async def _on_history(self):
        if not os.path.exists(self._cycles_path):
            await self._tg.send_alert("📋 아직 완료된 사이클 없음")
            return
        try:
            with open(self._cycles_path) as f:
                lines = f.readlines()[-5:]
        except Exception as e:
            await self._tg.send_alert(f"📋 사이클 로그 읽기 실패: {e}")
            return

        text = "📋 <b>최근 사이클 (최대 5건)</b>\n\n"
        for line in lines:
            try:
                c = json.loads(line)
                emoji = "🟢" if c.get("pnl", 0) >= 0 else "🔴"
                text += (
                    f"#{c.get('cycle_id')} {c.get('pair')} {c.get('direction')} {emoji}\n"
                    f"  PnL: ${c.get('pnl', 0):+.2f} | {c.get('exit_reason', '?')}\n\n"
                )
            except Exception:
                continue
        await self._tg.send_alert(text)

    async def _on_positions(self):
        """양 거래소 실시간 포지션/잔고 조회"""
        text = "📌 <b>현재 포지션</b>\n\n"
        # Dango
        try:
            bal = await self._dango.get_balance()
            text += f"<b>Dango</b> equity=${bal['equity']:,.2f}\n"
            pos = self.bot_state.position
            if pos and pos.pair:
                dango_pos = await self._dango.get_position(Config.DANGO_SYMBOL_MAP[pos.pair])
                if dango_pos:
                    text += f"  {pos.pair}: size={dango_pos.get('size', '?')}\n"
                else:
                    text += f"  {pos.pair}: 포지션 없음 (오더북 대기 중일 수 있음)\n"
        except Exception as e:
            text += f"<b>Dango</b> 조회 실패: {e}\n"
        text += "\n"
        # Hibachi
        try:
            hb_pos = await self._hb.get_positions()
            if hb_pos:
                for p in hb_pos:
                    text += f"<b>Hibachi</b> {p.get('symbol')}: {p.get('quantity')}\n"
            else:
                text += "<b>Hibachi</b> 포지션 없음\n"
        except Exception as e:
            text += f"<b>Hibachi</b> 조회 실패: {e}\n"
        await self._tg.send_alert(text)

    async def _on_resync(self):
        """수동 정리 후 강제 동기화 — 양쪽 실측이 dust 미만이면 즉시 사이클 종료."""
        pos = self.bot_state.position
        if not pos:
            await self._tg.send_alert("🔄 Resync: 활성 포지션 없음")
            return
        try:
            d_size = await self._dango.get_position_signed_size(Config.DANGO_SYMBOL_MAP[pos.pair])
            h_size = await self._hb.get_position_signed_size(Config.HIBACHI_SYMBOL_MAP[pos.pair])
        except Exception as e:
            await self._tg.send_alert(f"🔄 Resync 실패: {e}")
            return

        if await self._positions_at_dust(pos):
            await self._tg.send_alert(
                f"🔄 Resync: 양쪽 dust 이하 (dango={d_size:.6f} hibachi={h_size:.6f})"
                f" — 사이클 정상 종료"
            )
            await self._finalize_cycle(pos, reason_override="manual_resync")
        else:
            await self._tg.send_alert(
                f"🔄 Resync: 잔여 보유 — dango={d_size:.6f} hibachi={h_size:.6f}\n"
                f"수동 정리 후 다시 누르거나 🔚 Close Now 사용하시오"
            )

    async def _on_close_now(self):
        """현재 사이클 즉시 EXIT (HOLD 상태에서만)"""
        if self.bot_state.state != State.HOLD:
            await self._tg.send_alert(
                f"현재 상태({self.bot_state.state.value})에서는 강제 청산 불가. "
                f"HOLD 상태에서만 가능하옵니다."
            )
            return
        if not self.bot_state.position:
            await self._tg.send_alert("청산할 포지션 없음")
            return
        self.bot_state.position.exit_reason = "manual_close_now"
        self._transition(State.EXIT)
        await self._tg.send_alert("🔚 강제 청산 시작 — EXIT 진입")

    async def _on_stop(self):
        await self._tg.send_alert("⏹ 봇 종료 요청 수신 — 정리 후 종료하옵니다")
        self._stop_requested = True

    # ──────────────────────────────────────────────
    # 상태 디스패치
    # ──────────────────────────────────────────────

    async def _tick(self):
        s = self.bot_state.state
        if s == State.IDLE:
            await self._state_idle()
        elif s == State.ANALYZE:
            await self._state_analyze()
        elif s == State.ENTER:
            await self._state_enter()
        elif s == State.HOLD:
            await self._state_hold()
        elif s == State.HOLD_SUSPENDED:
            await self._state_hold_suspended()
        elif s == State.EXIT:
            await self._state_exit()
        elif s == State.COOLDOWN:
            await self._state_cooldown()
        elif s == State.MANUAL_INTERVENTION:
            await self._state_manual()

    # ──────────────────────────────────────────────
    # IDLE
    # ──────────────────────────────────────────────

    async def _state_idle(self):
        logger.info("IDLE — 펀딩레이트 스캔")
        funding = {}
        for pair in Config.PAIRS:
            try:
                dango_sym = Config.DANGO_SYMBOL_MAP[pair]
                hibachi_sym = Config.HIBACHI_SYMBOL_MAP[pair]
                dango_fr = await self._dango.get_funding_rate(dango_sym)
                hibachi_fr = await self._hb.get_funding_rate(hibachi_sym)
                funding[pair] = {"dango": dango_fr, "hibachi": hibachi_fr}
                logger.info("펀딩 %s: dango=%.5f hibachi=%.5f (8h)", pair, dango_fr, hibachi_fr)
            except Exception as e:
                logger.warning("펀딩 조회 실패 %s: %s", pair, e)

        if not funding:
            await asyncio.sleep(10)
            return

        pair, direction, net = select_pair_and_direction(funding)
        logger.info("선택: %s %s net=%.5f", pair, direction.value, net)

        # 잔고 확인
        try:
            bal = await self._dango.get_balance()
            equity = bal["equity"]
        except Exception as e:
            logger.warning("Dango 잔고 조회 실패: %s", e)
            await asyncio.sleep(5)
            return

        notional = calc_entry_notional(equity, Config.LEVERAGE)
        dango_sym = Config.DANGO_SYMBOL_MAP[pair]

        try:
            bbo = await self._dango.get_bbo(dango_sym)
            price = bbo["mark"]
        except Exception as e:
            logger.warning("BBO 조회 실패: %s", e)
            await asyncio.sleep(5)
            return

        try:
            h_bal = await self._hb.get_balance()
            h_equity = float(h_bal.get("balance", h_bal.get("equity", 0)) or 0)
        except Exception:
            h_equity = 0

        self.bot_state.position = Position(
            pair=pair,
            direction=direction,
            entry_balance=equity,
            entry_total_balance=equity + h_equity,
            target_notional=notional,
            avg_entry_price=price,
        )
        self._transition(State.ANALYZE)

    # ──────────────────────────────────────────────
    # ANALYZE
    # ──────────────────────────────────────────────

    async def _state_analyze(self):
        pos = self.bot_state.position
        logger.info("ANALYZE: %s %s notional=$%.0f", pos.pair, pos.direction.value, pos.target_notional)
        # 간단한 사전 체크 (추가 검증 필요 시 여기서)
        self._transition(State.ENTER)

    # ──────────────────────────────────────────────
    # ENTER — Dango-first XEMM 5청크
    # ──────────────────────────────────────────────

    async def _state_enter(self):
        pos = self.bot_state.position
        logger.info("ENTER 시작: %s %s", pos.pair, pos.direction.value)

        consecutive_failures = 0
        for chunk_idx in range(1, Config.ENTRY_CHUNKS + 1):
            success = await self._xemm_chunk(
                pos=pos,
                chunk_idx=chunk_idx,
                total_chunks=Config.ENTRY_CHUNKS,
                reduce_only=False,
            )
            if success:
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    logger.error("ENTER 청크 %d — 3회 연속 실패, 진입 중단", chunk_idx)
                    break
                logger.warning("ENTER 청크 %d 실패 — %d/3 허용, 다음 청크 시도", chunk_idx, consecutive_failures)

        # 진입 완료 후 실측 재동기화 — 양쪽 실제 포지션을 봇 상태에 반영
        # (nado_grvt acba425 패턴 — bot 내부 상태와 거래소 실측 불일치 방지)
        try:
            real_d = await self._dango.get_position_signed_size(Config.DANGO_SYMBOL_MAP[pos.pair])
            real_h = await self._hb.get_position_signed_size(Config.HIBACHI_SYMBOL_MAP[pos.pair])
            d_abs, h_abs = abs(real_d), abs(real_h)
            if abs(d_abs - h_abs) > min(d_abs, h_abs) * 0.05:
                logger.warning(
                    "진입 후 양쪽 사이즈 불일치 (dango=%.6f hibachi=%.6f, 차이>5%%) → 초과분 자동 조정",
                    d_abs, h_abs,
                )
                excess_exchange = "dango" if d_abs > h_abs else "hibachi"
                excess = abs(d_abs - h_abs)
                try:
                    if excess_exchange == "dango":
                        close_side = "SELL" if pos.dango_side == "BUY" else "BUY"
                        await self._dango.place_market_order(
                            Config.DANGO_SYMBOL_MAP[pos.pair], close_side, excess,
                            slippage=Config.EMERGENCY_CLOSE_SLIPPAGE_PCT,
                        )
                    else:
                        close_side = "SELL" if pos.hibachi_side == "BUY" else "BUY"
                        await self._hb.place_market_order(
                            Config.HIBACHI_SYMBOL_MAP[pos.pair], close_side, excess,
                        )
                    await asyncio.sleep(2)
                    adj_d = abs(await self._dango.get_position_signed_size(Config.DANGO_SYMBOL_MAP[pos.pair]))
                    adj_h = abs(await self._hb.get_position_signed_size(Config.HIBACHI_SYMBOL_MAP[pos.pair]))
                    pos.dango_size = adj_d
                    pos.hibachi_size = adj_h
                    logger.info("사이즈 자동 조정 완료: %s 초과 %.6f 청산 → dango=%.6f hibachi=%.6f", excess_exchange, excess, adj_d, adj_h)
                    await self._tg.send_alert(
                        f"[⚠️ ENTER 조정] {pos.pair} {excess_exchange} 초과 {excess:.6f} 자동 청산"
                    )
                except Exception as adj_err:
                    logger.error("사이즈 자동 조정 실패 → MANUAL: %s", adj_err)
                    pos.dango_size = d_abs
                    pos.hibachi_size = h_abs
                    self._transition(State.MANUAL_INTERVENTION)
                    return
            else:
                pos.dango_size = d_abs
                pos.hibachi_size = h_abs
        except Exception as e:
            logger.warning("진입 후 실측 동기화 실패 (내부 상태 유지): %s", e)

        if pos.dango_size > 0:
            await self._safe_get_balance()
            logger.info(
                "진입 완료: chunks=%d dango=%.6f hibachi=%.6f",
                pos.chunks_filled, pos.dango_size, pos.hibachi_size,
            )
            await self._tg.notify_enter(
                pos.pair, pos.direction.value, pos.target_notional,
                pos.avg_entry_price, self.bot_state.cycle_count + 1,
            )
            self._transition(State.HOLD)
        else:
            logger.warning("진입 체결 0건 — COOLDOWN")
            self.bot_state.position = None
            self._transition(State.COOLDOWN)

    # ──────────────────────────────────────────────
    # HOLD
    # ──────────────────────────────────────────────

    async def _state_hold(self):
        pos = self.bot_state.position
        await asyncio.sleep(30)

        # 외부 정리/수동 청산 인식 — 양쪽 실측 합이 dust 미만이면 사이클 정리
        if await self._positions_at_dust(pos):
            logger.info("HOLD 중 양쪽 dust 이하 감지 — 외부 정리로 간주, 사이클 종료")
            await self._tg.send_alert(
                f"[🔔 EXIT 결정] {pos.pair} | external_clear | 양쪽 dust 이하 감지"
            )
            await self._finalize_cycle(pos, reason_override="external_clear")
            return

        try:
            bal = await self._dango.get_balance()
            current_balance = bal["equity"]
        except Exception:
            current_balance = pos.entry_balance

        hibachi_margin = self._margin_monitor.margin_pct

        try:
            dango_sym = Config.DANGO_SYMBOL_MAP[pos.pair]
            bbo = await self._dango.get_bbo(dango_sym)
            mark = bbo["mark"]
            spread_mtm = _calc_spread_mtm(pos, mark)
        except Exception:
            spread_mtm = 0.0

        funding = await self._fetch_funding_for_pair(pos.pair)
        reason = should_exit(pos, current_balance, hibachi_margin, spread_mtm, funding)

        if reason:
            logger.info("청산 조건 충족: %s", reason)
            self.bot_state.position.exit_reason = reason
            self._transition(State.EXIT)
            await self._tg.send_alert(
                f"[🔔 EXIT 결정] {pos.pair} | {reason} | spread_mtm: ${spread_mtm:+.2f}"
            )

    # ──────────────────────────────────────────────
    # HOLD_SUSPENDED (Dango 장애)
    # ──────────────────────────────────────────────

    async def _state_hold_suspended(self):
        pos = self.bot_state.position
        hibachi_margin = self._margin_monitor.margin_pct

        # Dango 장애 중 + Hibachi 마진 위기 → Hibachi만 먼저 청산
        if hibachi_margin <= Config.MARGIN_EMERGENCY_PCT:
            logger.error("HOLD_SUSPENDED + 마진 긴급 → Hibachi 강제 청산")
            await self._emergency_close_hibachi_only(pos)
            return

        await asyncio.sleep(15)

    # ──────────────────────────────────────────────
    # EXIT — Dango-first XEMM 5청크
    # ──────────────────────────────────────────────

    async def _state_exit(self):
        pos = self.bot_state.position
        logger.info("EXIT 시작: %s %s reason=%s", pos.pair, pos.direction.value, pos.exit_reason)

        # 진입 과정에서 남은 미취소 주문 전부 정리
        try:
            await self._dango.cancel_all_orders(Config.DANGO_SYMBOL_MAP[pos.pair])
        except Exception as e:
            logger.warning("EXIT 시작 cancel_all 실패: %s", e)

        dango_sym = Config.DANGO_SYMBOL_MAP[pos.pair]
        hibachi_sym = Config.HIBACHI_SYMBOL_MAP[pos.pair]
        try:
            actual_pos = await self._dango.get_position_signed_size(dango_sym)
            total_to_exit = abs(actual_pos)
            if abs(total_to_exit - pos.dango_size) > pos.dango_size * 0.01:
                logger.warning("EXIT 초기 실측 보정: bot=%.6f → actual=%.6f", pos.dango_size, total_to_exit)
        except Exception:
            total_to_exit = pos.dango_size
        per_chunk = total_to_exit / Config.EXIT_CHUNKS if total_to_exit > 0 else 0
        exited = 0.0

        for chunk_idx in range(1, Config.EXIT_CHUNKS + 1):
            if await self._positions_at_dust(pos):
                logger.info("EXIT 중 dust 도달 — 청크 %d에서 조기 종료", chunk_idx)
                break

            try:
                actual_remain = abs(await self._dango.get_position_signed_size(dango_sym))
            except Exception:
                actual_remain = total_to_exit - exited
            remaining = min(total_to_exit - exited, actual_remain)
            if remaining < 1e-8:
                break

            chunk_size = min(per_chunk, remaining)
            exit_side = "SELL" if pos.dango_side == "BUY" else "BUY"
            success = await self._xemm_chunk_exit(
                pos=pos,
                chunk_idx=chunk_idx,
                total_chunks=Config.EXIT_CHUNKS,
                chunk_size=chunk_size,
                exit_side=exit_side,
            )
            if success:
                exited += chunk_size
                self.bot_state.exit_failure_count = 0
            else:
                self.bot_state.exit_failure_count += 1
                # Hibachi 잔여분 불균형이면 다음 Dango 청크 전에 Hibachi만 재시도
                imbalance = abs(pos.dango_size - pos.hibachi_size)
                if imbalance > 1e-6:
                    logger.warning(
                        "EXIT 헷지 불균형 (dango=%.6f hibachi=%.6f) → Hibachi 재매칭 시도",
                        pos.dango_size, pos.hibachi_size,
                    )
                    await asyncio.sleep(10)
                    matched = await self._retry_hibachi_match(pos, imbalance)
                    if matched:
                        logger.info("Hibachi 재매칭 성공 — EXIT 계속")
                        self.bot_state.exit_failure_count = 0
                        continue
                if self.bot_state.exit_failure_count >= Config.MAX_EXIT_FAILURES:
                    self._transition(State.MANUAL_INTERVENTION)
                    return

        # 청산 후 실측 확정 — 잔여가 dust 초과면 MANUAL
        if not await self._positions_at_dust(pos):
            d_remain = await self._dango.get_position_signed_size(Config.DANGO_SYMBOL_MAP[pos.pair])
            h_remain = await self._hb.get_position_signed_size(Config.HIBACHI_SYMBOL_MAP[pos.pair])
            logger.warning(
                "EXIT 5청크 완료 후 잔여 dust 초과 — dango=%.6f hibachi=%.6f → MANUAL",
                d_remain, h_remain,
            )
            self._transition(State.MANUAL_INTERVENTION)
            return

        await self._finalize_cycle(pos)

    async def _finalize_cycle(self, pos: Position, reason_override: Optional[str] = None):
        """사이클 정상 종료 — Cycle 기록 + 텔레그램 알림 + COOLDOWN 전환."""
        self.bot_state.exit_failure_count = 0
        bal = await self._safe_get_balance()
        pnl = (bal - pos.entry_balance) if bal else 0.0
        self.bot_state.cycle_count += 1

        reason = reason_override or pos.exit_reason or "unknown"
        cycle = Cycle(
            cycle_id=self.bot_state.cycle_count,
            pair=pos.pair,
            direction=pos.direction.value,
            entry_balance=pos.entry_balance,
            exit_balance=bal or pos.entry_balance,
            pnl=pnl,
            exit_reason=reason,
            exit_time=time.time(),
            chunks=pos.chunks_filled,
        )
        self._save_cycle(cycle)
        await self._tg.notify_exit(reason, pnl, self.bot_state.cycle_count)

        self.bot_state.position = None
        self._transition(State.COOLDOWN)

    async def _positions_at_dust(self, pos: Position) -> bool:
        """양 거래소 실측 포지션의 합 notional이 DUST_NOTIONAL_USD 미만이면 True.
        외부 정리/수동 청산 인식용."""
        try:
            d_size = await self._dango.get_position_signed_size(Config.DANGO_SYMBOL_MAP[pos.pair])
            h_size = await self._hb.get_position_signed_size(Config.HIBACHI_SYMBOL_MAP[pos.pair])
        except Exception as e:
            logger.debug("dust 체크 조회 실패: %s", e)
            return False
        # notional 추정 (mark 기준)
        try:
            mark = await self._dango.get_mark_price(Config.DANGO_SYMBOL_MAP[pos.pair])
        except Exception:
            mark = pos.avg_entry_price
        d_notional = abs(d_size) * mark
        h_notional = abs(h_size) * mark
        return d_notional < Config.DUST_NOTIONAL_USD and h_notional < Config.DUST_NOTIONAL_USD

    # ──────────────────────────────────────────────
    # COOLDOWN
    # ──────────────────────────────────────────────

    async def _state_cooldown(self):
        logger.info("COOLDOWN %d분 대기", Config.COOLDOWN_MINUTES)
        await asyncio.sleep(Config.COOLDOWN_MINUTES * 60)
        self._transition(State.IDLE)

    # ──────────────────────────────────────────────
    # MANUAL_INTERVENTION
    # ──────────────────────────────────────────────

    async def _state_manual(self):
        now = time.time()
        if now - self.bot_state.last_manual_alert > 1800:
            await self._tg.notify_manual_intervention(
                self.bot_state.exit_failure_count, self.bot_state.cycle_count
            )
            self.bot_state.last_manual_alert = now
        await asyncio.sleep(60)

    # ──────────────────────────────────────────────
    # XEMM 청크 실행 (진입)
    # ──────────────────────────────────────────────

    async def _xemm_chunk(
        self, pos: Position, chunk_idx: int, total_chunks: int, reduce_only: bool
    ) -> bool:
        """
        1청크 진입: Dango maker → 실측 체결량으로 Hibachi taker 헷지.

        안전 패턴 (Bug C 방지, P0+P1 적용):
        1. 매 시도마다 포지션 사이즈 before/after 비교 → 실제 체결량 산정
        2. 항상 cancel 후 잔량 정리 (race 방지)
        3. Hibachi 헷지 부족 시 Dango 반대 방향으로 롤백
        4. MAKER_RETRY_LIMIT 초과 시 종료 (무한 루프 방지)
        """
        dango_sym = Config.DANGO_SYMBOL_MAP[pos.pair]
        hibachi_sym = Config.HIBACHI_SYMBOL_MAP[pos.pair]
        chunk_size = calc_chunk_size(pos.target_notional, pos.avg_entry_price, total_chunks)
        tick = Config.PAIR_TICK_SIZE.get(pos.pair, 0.01)
        d_sign = 1.0 if pos.dango_side == "BUY" else -1.0
        h_sign = 1.0 if pos.hibachi_side == "BUY" else -1.0

        concession = 0.0

        for retry in range(Config.MAKER_RETRY_LIMIT):
            if self._health_monitor.is_down:
                logger.warning("Dango 다운 — 청크 %d 중단", chunk_idx)
                return False

            try:
                bbo = await self._dango.get_bbo(dango_sym)
            except Exception as e:
                logger.warning("BBO 조회 실패: %s", e)
                await asyncio.sleep(2)
                continue
            spread = bbo["ask"] - bbo["bid"]
            # Maker 가격 로직 — POST_ONLY cross 방지 (Dango 에러 "would cross best bid" 회귀 방지)
            if pos.dango_side == "BUY":
                raw = bbo["bid"] + concession
                raw = min(raw, bbo["ask"] - tick)
            else:
                raw = bbo["ask"] - concession
                raw = max(raw, bbo["bid"] + tick)
            price = _quantize_to_tick(raw, tick)
            if pos.dango_side == "BUY":
                price = min(price, _quantize_to_tick(bbo["ask"] - tick, tick))
            else:
                price = max(price, _quantize_to_tick(bbo["bid"] + tick, tick))

            cid = self._dango.make_client_order_id()

            try:
                size_before = await self._dango.get_position_signed_size(dango_sym)
            except Exception:
                size_before = 0.0

            try:
                await self._dango.place_limit_order(
                    dango_sym, pos.dango_side, price, chunk_size,
                    reduce_only=reduce_only, post_only=True, client_order_id=cid,
                )
            except Exception as e:
                logger.warning("Dango 주문 실패: %s", e)
                await asyncio.sleep(2)
                continue

            fill_event = await self._dango.wait_for_fill(cid, timeout=Config.MAKER_FILL_TIMEOUT_SECONDS)

            # cancel_all — 잔여 maker 전부 정리 (deliver_tx 검증 포함)
            cancel_ok = False
            try:
                result = await self._dango.cancel_all_orders(dango_sym)
                cancel_ok = bool(result)
            except Exception as e:
                logger.warning("Dango cancel_all 실패: %s", e)
            if not cancel_ok:
                logger.error("cancel_all 실패 — 잔여 주문 존재 위험, 청크 중단")
                return False
            await asyncio.sleep(2)

            try:
                size_after = await self._dango.get_position_signed_size(dango_sym)
            except Exception as e:
                logger.warning("Dango 포지션 조회 실패: %s", e)
                size_after = size_before  # 보수적: 미체결로 간주

            actual_filled = max(0.0, (size_after - size_before) * d_sign)

            if actual_filled < chunk_size * 0.01:
                # 사실상 미체결 — 가격 양보 후 재시도 (hard cap이 cross 방지)
                concession += max(Config.MAKER_PRICE_STEP_USD, tick)
                continue

            fill_price = float(fill_event.get("fill_price", price)) if fill_event else price

            # Hibachi taker (실측 체결량으로 헷지)
            try:
                hb_size_before = await self._hb.get_position_signed_size(hibachi_sym)
                slip = 1.005 if pos.hibachi_side == "BUY" else 0.995
                hb_tick = Config.HIBACHI_TICK_SIZE.get(pos.pair, 0.01)
                hb_taker_price = _quantize_to_tick(fill_price * slip, hb_tick)
                await self._hb.place_limit_order(
                    hibachi_sym, pos.hibachi_side, hb_taker_price,
                    actual_filled, post_only=False,
                )
                # Hibachi 포지션 반영 대기 — 1초는 부족 (delta_donemoji 8초 기준)
                # 3초 대기 + 2회 폴링 재시도로 안정적 확인
                hb_filled = 0.0
                for poll in range(3):
                    await asyncio.sleep(3)
                    hb_size_after = await self._hb.get_position_signed_size(hibachi_sym)
                    hb_filled = max(0.0, (hb_size_after - hb_size_before) * h_sign)
                    if hb_filled >= actual_filled * 0.9:
                        break
                    logger.debug("Hibachi 폴링 %d/3: filled=%.6f/%.6f", poll + 1, hb_filled, actual_filled)
                if hb_filled < actual_filled * 0.9:
                    raise RuntimeError(
                        f"Hibachi 헷지 부족: {hb_filled:.6f}/{actual_filled:.6f}"
                    )
            except Exception as e:
                logger.error("Hibachi 헷지 실패 — Dango 롤백 (반대 방향 시장가): %s", e)
                rollback_side = "SELL" if pos.dango_side == "BUY" else "BUY"
                try:
                    await self._dango.place_market_order(
                        dango_sym, rollback_side, actual_filled,
                        slippage=Config.EMERGENCY_CLOSE_SLIPPAGE_PCT,
                    )
                except Exception as re:
                    logger.critical(
                        "Dango 롤백 실패! 수동 개입 필요 (size=%.6f side=%s): %s",
                        actual_filled, rollback_side, re,
                    )
                return False

            # 성공 — 체결량 동기화 (일치 보장된 값 사용)
            matched = min(actual_filled, hb_filled)
            pos.dango_size += matched
            pos.hibachi_size += matched
            pos.chunks_filled += 1
            pos.avg_entry_price = (
                (pos.avg_entry_price * (chunk_idx - 1) + fill_price) / chunk_idx
            )
            logger.info(
                "청크 %d/%d 완료: matched=%.6f (dango=%.6f hibachi=%.6f) @ %.2f retries=%d",
                chunk_idx, total_chunks, matched, actual_filled, hb_filled,
                fill_price, retry,
            )
            return True

        logger.warning(
            "청크 %d MAKER_RETRY_LIMIT(%d) 소진 — 미체결",
            chunk_idx, Config.MAKER_RETRY_LIMIT,
        )
        return False

    # ──────────────────────────────────────────────
    # 청산 청크 실행 (maker 5회 → 시장가 fallback)
    # ──────────────────────────────────────────────

    async def _xemm_chunk_exit(
        self, pos: Position, chunk_idx: int, total_chunks: int,
        chunk_size: float, exit_side: str,
    ) -> bool:
        """1청크 청산: Dango maker 5회 시도 → 전부 미체결 시 시장가 fallback.
        양쪽 체결 후 Hibachi taker로 반대 청산."""
        dango_sym = Config.DANGO_SYMBOL_MAP[pos.pair]
        hibachi_sym = Config.HIBACHI_SYMBOL_MAP[pos.pair]
        hibachi_close_side = "BUY" if pos.hibachi_side == "SELL" else "SELL"
        d_exit_sign = -1.0 if exit_side == "SELL" else 1.0
        h_close_sign = 1.0 if hibachi_close_side == "BUY" else -1.0
        tick = Config.PAIR_TICK_SIZE.get(pos.pair, 0.01)

        actual_filled = 0.0
        fill_price = pos.avg_entry_price

        # ── Phase 1: Maker 시도 (최대 MAKER_RETRY_LIMIT회) ──
        # EXIT는 체결 최우선 — 처음부터 최공격적 POST_ONLY 가격
        # BUY: ask 바로 아래 (최고 bid가 되어 taker 유인)
        # SELL: bid 바로 위 (최저 ask가 되어 taker 유인)
        for retry in range(Config.MAKER_RETRY_LIMIT):
            if self._health_monitor.is_down:
                logger.warning("Dango 다운 — EXIT 청크 %d 중단", chunk_idx)
                return False

            try:
                bbo = await self._dango.get_bbo(dango_sym)
            except Exception as e:
                logger.warning("EXIT BBO 조회 실패: %s", e)
                await asyncio.sleep(2)
                continue

            if exit_side == "BUY":
                price = _quantize_to_tick(bbo["ask"] - tick, tick)
            else:
                price = _quantize_to_tick(bbo["bid"] + tick, tick)

            cid = self._dango.make_client_order_id()

            try:
                size_before = await self._dango.get_position_signed_size(dango_sym)
            except Exception:
                size_before = 0.0

            try:
                await self._dango.place_limit_order(
                    dango_sym, exit_side, price, chunk_size,
                    reduce_only=True, post_only=True, client_order_id=cid,
                )
            except Exception as e:
                logger.warning("EXIT Dango maker 실패: %s", e)
                await asyncio.sleep(2)
                continue

            await self._dango.wait_for_fill(cid, timeout=Config.MAKER_FILL_TIMEOUT_SECONDS)

            cancel_ok = False
            try:
                result = await self._dango.cancel_all_orders(dango_sym)
                cancel_ok = bool(result)
            except Exception as e:
                logger.warning("EXIT cancel_all 실패: %s", e)
            if not cancel_ok:
                logger.error("EXIT cancel_all 실패 — 잔여 주문 위험, 청크 중단")
                return False
            await asyncio.sleep(2)

            try:
                size_after = await self._dango.get_position_signed_size(dango_sym)
            except Exception:
                size_after = size_before

            actual_filled = max(0.0, (size_before - size_after) * (-d_exit_sign))
            if actual_filled >= chunk_size * 0.01:
                fill_price = price
                break

        # ── Phase 2: Maker 전부 미체결 → 시장가 fallback ──
        if actual_filled < chunk_size * 0.01:
            logger.warning("EXIT 청크 %d maker %d회 미체결 → 시장가 fallback", chunk_idx, Config.MAKER_RETRY_LIMIT)
            try:
                size_before = await self._dango.get_position_signed_size(dango_sym)
            except Exception:
                size_before = 0.0

            try:
                await self._dango.place_market_order(
                    dango_sym, exit_side, chunk_size,
                    slippage=Config.EMERGENCY_CLOSE_SLIPPAGE_PCT,
                )
            except Exception as e:
                logger.error("EXIT Dango 시장가도 실패 (청크 %d): %s", chunk_idx, e)
                return False

            await asyncio.sleep(3)

            try:
                size_after = await self._dango.get_position_signed_size(dango_sym)
            except Exception:
                size_after = size_before

            actual_filled = max(0.0, (size_before - size_after) * (-d_exit_sign))
            if actual_filled < chunk_size * 0.01:
                logger.error("EXIT 청크 %d 시장가도 미체결", chunk_idx)
                return False

            try:
                fill_price = await self._dango.get_mark_price(dango_sym)
            except Exception:
                fill_price = pos.avg_entry_price

        # ── Phase 3: Hibachi taker 청산 (2회 시도 — 실패 시 넓은 슬리피지 재시도) ──
        hb_closed_total = 0.0
        try:
            hb_size_origin = await self._hb.get_position_signed_size(hibachi_sym)
        except Exception as e:
            logger.error("EXIT Hibachi 포지션 조회 실패 (Dango %.6f 이미 청산됨): %s", actual_filled, e)
            pos.dango_size = max(0.0, pos.dango_size - actual_filled)
            await self._tg.send_alert(
                f"[🚨 EXIT 불균형] Hibachi API 장애! Dango {actual_filled:.6f} 청산됨 — 즉시 확인!"
            )
            return False
        hb_slippages = [0.005, 0.015]
        for hb_attempt, slip_pct in enumerate(hb_slippages, 1):
            hb_remaining = actual_filled - hb_closed_total
            if hb_remaining < 1e-8:
                break
            try:
                hb_size_before = await self._hb.get_position_signed_size(hibachi_sym)
                slip = (1 + slip_pct) if hibachi_close_side == "BUY" else (1 - slip_pct)
                hb_tick = Config.HIBACHI_TICK_SIZE.get(pos.pair, 0.01)
                hb_taker_price = _quantize_to_tick(fill_price * slip, hb_tick)
                await self._hb.place_limit_order(
                    hibachi_sym, hibachi_close_side, hb_taker_price,
                    hb_remaining, post_only=False,
                )
                for poll in range(3):
                    await asyncio.sleep(3)
                    hb_size_now = await self._hb.get_position_signed_size(hibachi_sym)
                    hb_closed_total = max(0.0, (hb_size_now - hb_size_origin) * h_close_sign)
                    if hb_closed_total >= actual_filled * 0.9:
                        break
                if hb_closed_total >= actual_filled * 0.9:
                    break
                logger.warning(
                    "EXIT Hibachi 시도 %d/%d 부족: %.6f/%.6f (slip=%.1f%%)",
                    hb_attempt, len(hb_slippages), hb_closed_total, actual_filled, slip_pct * 100,
                )
            except Exception as e:
                logger.warning("EXIT Hibachi 시도 %d/%d 실패: %s", hb_attempt, len(hb_slippages), e)

        pos.dango_size = max(0.0, pos.dango_size - actual_filled)
        pos.hibachi_size = max(0.0, pos.hibachi_size - hb_closed_total)

        if hb_closed_total < actual_filled * 0.5:
            logger.error(
                "EXIT Hibachi 청산 심각 부족 — 청크 %d dango=%.6f hibachi=%.6f → 헷지 불균형!",
                chunk_idx, actual_filled, hb_closed_total,
            )
            await self._tg.send_alert(
                f"[🚨 EXIT 불균형] 청크 {chunk_idx}: Dango {actual_filled:.6f} 청산, "
                f"Hibachi {hb_closed_total:.6f}만 청산 — 확인 필요!"
            )
            return False

        logger.info(
            "EXIT 청크 %d/%d: dango=%.6f hibachi=%.6f @ %.2f",
            chunk_idx, total_chunks, actual_filled, hb_closed_total, fill_price,
        )
        return True

    async def _retry_hibachi_match(self, pos: Position, deficit: float) -> bool:
        """Hibachi 청산 부족분 재매칭. Dango는 이미 청산됨, Hibachi만 추가 청산."""
        hibachi_sym = Config.HIBACHI_SYMBOL_MAP[pos.pair]
        close_side = "BUY" if pos.hibachi_side == "SELL" else "SELL"
        try:
            mark = await self._hb.get_mark_price(hibachi_sym)
            hb_tick = Config.HIBACHI_TICK_SIZE.get(pos.pair, 0.01)
            slip = 1.015 if close_side == "BUY" else 0.985
            price = _quantize_to_tick(mark * slip, hb_tick)
            hb_before = await self._hb.get_position_signed_size(hibachi_sym)
            await self._hb.place_limit_order(
                hibachi_sym, close_side, price, deficit, post_only=False,
            )
            h_sign = 1.0 if close_side == "BUY" else -1.0
            for _ in range(4):
                await asyncio.sleep(3)
                hb_after = await self._hb.get_position_signed_size(hibachi_sym)
                closed = max(0.0, (hb_after - hb_before) * h_sign)
                if closed >= deficit * 0.8:
                    pos.hibachi_size = max(0.0, pos.hibachi_size - closed)
                    logger.info("Hibachi 재매칭 완료: %.6f/%.6f", closed, deficit)
                    return True
            logger.warning("Hibachi 재매칭 부족: deficit=%.6f", deficit)
            return False
        except Exception as e:
            logger.error("Hibachi 재매칭 실패: %s", e)
            return False

    # ──────────────────────────────────────────────
    # 긴급 Hibachi-only 청산
    # ──────────────────────────────────────────────

    async def _emergency_close_hibachi_only(self, pos: Position):
        """Dango 다운 + 마진 긴급: Hibachi만 시장가 청산"""
        hibachi_sym = Config.HIBACHI_SYMBOL_MAP[pos.pair]
        hibachi_close_side = "BUY" if pos.hibachi_side == "SELL" else "SELL"
        try:
            await self._hb.place_limit_order(
                hibachi_sym, hibachi_close_side,
                await self._hb.get_mark_price(hibachi_sym),
                pos.hibachi_size, post_only=False,
            )
            logger.error("Hibachi 긴급 청산 완료. Dango 수동 청산 필요!")
            await self._tg.notify_hibachi_closed_only(pos.dango_size)
        except Exception as e:
            logger.error("Hibachi 긴급 청산 실패: %s", e)

    # ──────────────────────────────────────────────
    # 모니터 콜백
    # ──────────────────────────────────────────────

    async def _on_margin_emergency(self, pct: float):
        await self._tg.notify_margin_warning("Hibachi", pct)
        s = self.bot_state.state
        if s == State.HOLD:
            self.bot_state.position.exit_reason = f"MARGIN_EMERGENCY ({pct:.1f}%)"
            self._transition(State.EXIT)

    async def _on_dango_down(self):
        await self._tg.notify_dango_down()
        if self.bot_state.state == State.HOLD:
            self.bot_state.suspended_since = time.time()
            self._transition(State.HOLD_SUSPENDED)

    async def _on_dango_up(self):
        await self._tg.notify_dango_up()
        if self.bot_state.state == State.HOLD_SUSPENDED:
            self.bot_state.suspended_since = None
            self._transition(State.HOLD)

    # ──────────────────────────────────────────────
    # 크래시 복구
    # ──────────────────────────────────────────────

    async def _recovery_check(self):
        """봇 재시작 시 기존 포지션 복구 — 양쪽 거래소 실측 기반."""
        for pair in Config.PAIRS:
            dango_sym = Config.DANGO_SYMBOL_MAP[pair]
            hibachi_sym = Config.HIBACHI_SYMBOL_MAP[pair]

            try:
                d_size = await self._dango.get_position_signed_size(dango_sym)
                h_size = await self._hb.get_position_signed_size(hibachi_sym)
            except Exception as e:
                logger.warning("Recovery: 포지션 조회 실패 (%s): %s", pair, e)
                continue

            try:
                mark = await self._dango.get_mark_price(dango_sym)
            except Exception:
                mark = 0

            d_notional = abs(d_size) * mark
            h_notional = abs(h_size) * mark

            if d_notional < Config.DUST_NOTIONAL_USD and h_notional < Config.DUST_NOTIONAL_USD:
                continue

            if d_notional >= Config.DUST_NOTIONAL_USD and h_notional >= Config.DUST_NOTIONAL_USD:
                # 양쪽 같은 방향이면 delta neutral 아님 — 수동 확인
                if (d_size > 0 and h_size > 0) or (d_size < 0 and h_size < 0):
                    logger.critical(
                        "Recovery: 양쪽 같은 방향! dango=%.6f hibachi=%.6f",
                        d_size, h_size,
                    )
                    await self._tg.send_alert(
                        f"🚨 [RECOVERY] {pair} 양쪽 동일 방향 — 수동 확인 필요\n"
                        f"Dango: {d_size:.6f} / Hibachi: {h_size:.6f}"
                    )
                    continue

                direction = Direction.A if d_size > 0 else Direction.B
                try:
                    bal = await self._dango.get_balance()
                    equity = bal["equity"]
                except Exception:
                    equity = 0
                try:
                    h_bal = await self._hb.get_balance()
                    h_equity = float(h_bal.get("balance", h_bal.get("equity", 0)) or 0)
                except Exception:
                    h_equity = 0

                pos = Position(
                    pair=pair,
                    direction=direction,
                    entry_balance=equity,
                    entry_total_balance=equity + h_equity,
                    target_notional=d_notional,
                    dango_size=abs(d_size),
                    hibachi_size=abs(h_size),
                    avg_entry_price=mark,
                    chunks_filled=Config.ENTRY_CHUNKS,
                    entry_time=time.time(),
                )
                self.bot_state.position = pos
                self._transition(State.HOLD)
                logger.info(
                    "Recovery: %s 포지션 복구 → HOLD (dango=%.6f hibachi=%.6f)",
                    pair, abs(d_size), abs(h_size),
                )
                await self._tg.send_alert(
                    f"🔁 [RECOVERY] {pair} 포지션 복구 → HOLD\n"
                    f"Dango: {abs(d_size):.6f} ({direction.value})\n"
                    f"Hibachi: {abs(h_size):.6f}\n"
                    f"Mark: ${mark:,.2f}"
                )
                return

            side = "Dango" if d_notional >= Config.DUST_NOTIONAL_USD else "Hibachi"
            logger.warning("Recovery: %s 편측 포지션 감지 (%s)", pair, side)
            await self._tg.send_alert(
                f"⚠️ [RECOVERY] {pair} {side}에만 포지션 존재!\n"
                f"Dango: {d_size:.6f} / Hibachi: {h_size:.6f}\n"
                f"수동 확인 필요"
            )

        logger.info("Recovery: 포지션 없음 — IDLE 유지")

    # ──────────────────────────────────────────────
    # 유틸리티
    # ──────────────────────────────────────────────

    def _transition(self, new_state: State):
        old = self.bot_state.state
        self.bot_state.state = new_state
        logger.info("상태 전환: %s → %s", old.value, new_state.value)
        self.bot_state.save(self._state_path)

    async def _safe_get_balance(self) -> Optional[float]:
        try:
            bal = await self._dango.get_balance()
            return bal["equity"]
        except Exception:
            return None

    async def _fetch_funding_for_pair(self, pair: str) -> dict:
        try:
            dango_fr = await self._dango.get_funding_rate(Config.DANGO_SYMBOL_MAP[pair])
            hibachi_fr = await self._hb.get_funding_rate(Config.HIBACHI_SYMBOL_MAP[pair])
            return {pair: {"dango": dango_fr, "hibachi": hibachi_fr}}
        except Exception:
            return {}

    def _save_cycle(self, cycle: Cycle):
        with open(self._cycles_path, "a") as f:
            f.write(cycle.to_jsonl() + "\n")

    def _aggregate_realized(self) -> dict:
        """cycles.jsonl에서 누적 실현 PnL 집계"""
        result = {"count": 0, "pnl": 0.0}
        if not os.path.exists(self._cycles_path):
            return result
        try:
            with open(self._cycles_path) as f:
                for line in f:
                    try:
                        c = json.loads(line)
                        result["count"] += 1
                        result["pnl"] += c.get("pnl", 0.0)
                    except Exception:
                        continue
        except Exception:
            pass
        return result

    async def _shutdown(self):
        logger.info("엔진 종료 중...")
        await self._margin_monitor.stop()
        await self._health_monitor.stop()
        await self._dango.stop()
        await self._tg.stop()


def _calc_spread_mtm(pos: Position, mark_price: float) -> float:
    """스프레드 MTM 추정 (USD). 진입가 대비 현재 마크 가격 차이."""
    if pos.direction == Direction.A:
        # Dango LONG: mark > entry → 이익
        return (mark_price - pos.avg_entry_price) * pos.dango_size
    else:
        return (pos.avg_entry_price - mark_price) * pos.dango_size
