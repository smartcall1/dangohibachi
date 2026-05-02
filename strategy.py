"""전략 로직 — 방향 결정 및 청산 조건 평가"""
from __future__ import annotations

import logging
import time
from typing import Optional

from config import Config
from models import Direction, Position

logger = logging.getLogger(__name__)


def select_pair_and_direction(
    funding_rates: dict[str, dict[str, float]]
) -> tuple[str, Direction, float]:
    """
    최적 페어와 방향 선택.

    funding_rates = {
      "ETH": {"dango": 0.0003, "hibachi": 0.0001},
      "BTC": {"dango": 0.0002, "hibachi": 0.0004},
    }

    반환: (pair, direction, net_funding_8h)
    """
    best_pair = None
    best_direction = None
    best_score = float("-inf")

    for pair, rates in funding_rates.items():
        dango_fr = rates.get("dango", 0.0)
        hibachi_fr = rates.get("hibachi", 0.0)

        # Option A: Dango LONG + Hibachi SHORT
        # → Dango가 SHORT에게 펀딩 받음 + Hibachi는 LONG에게 지급
        net_a = dango_fr - hibachi_fr   # 음수면 우리가 지급 순손실

        # Option B: Dango SHORT + Hibachi LONG
        # → Dango가 LONG에게 펀딩 받음 + Hibachi는 SHORT에게 지급
        net_b = hibachi_fr - dango_fr

        # 거래량 봇: 둘 다 음수여도 덜 손해인 방향 선택
        if net_a >= net_b:
            score = net_a
            direction = Direction.A
        else:
            score = net_b
            direction = Direction.B

        logger.debug("페어 %s: net_A=%.5f net_B=%.5f → %s score=%.5f",
                     pair, net_a, net_b, direction.value, score)

        if score > best_score:
            best_score = score
            best_pair = pair
            best_direction = direction

    return best_pair, best_direction, best_score


def should_exit(
    position: Position,
    current_balance: float,
    hibachi_margin_pct: float,
    spread_mtm_usd: float,
    funding_rates: dict[str, float],
) -> Optional[str]:
    """
    청산 조건 평가. 청산해야 하면 이유 문자열 반환, 아니면 None.

    우선순위:
      1. Hibachi 마진 긴급
      2. 원금 회수 (잔고 회복)
      3. 기회적 익절 (스프레드 MTM)
      4. 최대 보유 기간 초과
      5. 펀딩 방향 역전 + MIN_HOLD 경과
    """
    cfg = Config

    # 1순위: Hibachi 마진 긴급
    if hibachi_margin_pct <= cfg.MARGIN_EMERGENCY_PCT:
        return f"MARGIN_EMERGENCY (Hibachi {hibachi_margin_pct:.1f}%)"

    min_hold_elapsed = position.hold_minutes >= cfg.MIN_HOLD_MINUTES

    # 2순위: 원금 회수
    required = position.entry_balance + _round_trip_fee(position) + cfg.PRINCIPAL_BUFFER_USD
    if current_balance >= required:
        return f"PRINCIPAL_RECOVERY (잔고 {current_balance:.2f} ≥ {required:.2f})"

    # 3순위: 기회적 익절
    if min_hold_elapsed and spread_mtm_usd >= cfg.SPREAD_OPPORTUNISTIC_USD:
        return f"OPPORTUNISTIC_PROFIT (MTM +{spread_mtm_usd:.2f})"

    # 4순위: 최대 보유 기간
    if position.hold_days >= cfg.MAX_HOLD_DAYS:
        return f"MAX_HOLD ({position.hold_days:.1f}일)"

    # 5순위: 펀딩 방향 역전
    if min_hold_elapsed:
        pair_fr = funding_rates.get(position.pair, {})
        if _funding_reversed(position.direction, pair_fr):
            return "FUNDING_REVERSED"

    return None


def _round_trip_fee(position: Position) -> float:
    """왕복 수수료 추정 (USD)"""
    return position.target_notional * Config.ROUND_TRIP_FEE_RATE


def _funding_reversed(direction: Direction, rates: dict[str, float]) -> bool:
    """현재 방향의 펀딩 순손실이 반대 방향보다 커졌으면 True"""
    dango_fr = rates.get("dango", 0.0)
    hibachi_fr = rates.get("hibachi", 0.0)
    if direction == Direction.A:
        net = dango_fr - hibachi_fr
    else:
        net = hibachi_fr - dango_fr
    # 손실이 커지고 있으면 탈출
    return net < -0.0003   # 8h 기준 -0.03% 이상 손해 시


def calc_chunk_size(total_notional: float, price: float, num_chunks: int) -> float:
    """청크당 수량 (base asset 단위)"""
    return round(total_notional / num_chunks / price, 6)


def calc_entry_notional(equity: float, leverage: int) -> float:
    """레버리지 적용 명목 포지션 크기 (USD)"""
    return equity * leverage
