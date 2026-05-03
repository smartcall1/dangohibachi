"""데이터 모델 — BotState, Position, Cycle"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional


class State(str, Enum):
    IDLE = "IDLE"
    ANALYZE = "ANALYZE"
    ENTER = "ENTER"
    HOLD = "HOLD"
    HOLD_SUSPENDED = "HOLD_SUSPENDED"
    EXIT = "EXIT"
    COOLDOWN = "COOLDOWN"
    MANUAL_INTERVENTION = "MANUAL_INTERVENTION"


class Direction(str, Enum):
    A = "A"  # Dango LONG + Hibachi SHORT
    B = "B"  # Dango SHORT + Hibachi LONG


@dataclass
class ChunkFill:
    dango_size: float
    dango_price: float
    hibachi_size: float
    hibachi_price: float
    timestamp: float = field(default_factory=time.time)


@dataclass
class Position:
    pair: str                    # "ETH" or "BTC"
    direction: Direction
    entry_balance: float         # 진입 시 Dango 잔고 (원금 회수 기준)
    target_notional: float       # 목표 명목 포지션 크기 (USD)
    entry_total_balance: float = 0.0  # 진입 시 양쪽 합산 잔고 (Status PnL 표시용)
    dango_size: float = 0.0      # 실제 체결된 Dango 포지션 크기
    hibachi_size: float = 0.0    # 실제 체결된 Hibachi 포지션 크기
    avg_entry_price: float = 0.0
    chunks_filled: int = 0
    entry_time: float = field(default_factory=time.time)
    fills: list[ChunkFill] = field(default_factory=list)
    exit_reason: str = ""        # EXIT 트리거 사유 (Status UI / Cycle 기록용)

    @property
    def dango_symbol(self) -> str:
        return {"ETH": "perp/ethusd", "BTC": "perp/btcusd"}[self.pair]

    @property
    def hibachi_symbol(self) -> str:
        return {"ETH": "ETH/USDT-P", "BTC": "BTC/USDT-P"}[self.pair]

    @property
    def dango_side(self) -> str:
        return "BUY" if self.direction == Direction.A else "SELL"

    @property
    def hibachi_side(self) -> str:
        return "SELL" if self.direction == Direction.A else "BUY"

    @property
    def hold_minutes(self) -> float:
        return (time.time() - self.entry_time) / 60

    @property
    def hold_days(self) -> float:
        return self.hold_minutes / 1440


@dataclass
class Cycle:
    cycle_id: int
    pair: str
    direction: str
    entry_balance: float
    exit_balance: float = 0.0
    pnl: float = 0.0
    exit_reason: str = ""
    entry_time: float = field(default_factory=time.time)
    exit_time: float = 0.0
    chunks: int = 0

    def to_jsonl(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class BotState:
    state: State = State.IDLE
    position: Optional[Position] = None
    cycle_count: int = 0
    exit_failure_count: int = 0
    suspended_since: Optional[float] = None  # HOLD_SUSPENDED 진입 시각
    last_manual_alert: float = 0.0

    def to_dict(self) -> dict:
        d = {
            "state": self.state.value,
            "cycle_count": self.cycle_count,
            "exit_failure_count": self.exit_failure_count,
            "suspended_since": self.suspended_since,
            "last_manual_alert": self.last_manual_alert,
        }
        if self.position:
            p = self.position
            d["position"] = {
                "pair": p.pair,
                "direction": p.direction.value,
                "entry_balance": p.entry_balance,
                "entry_total_balance": p.entry_total_balance,
                "target_notional": p.target_notional,
                "dango_size": p.dango_size,
                "hibachi_size": p.hibachi_size,
                "avg_entry_price": p.avg_entry_price,
                "chunks_filled": p.chunks_filled,
                "entry_time": p.entry_time,
            }
        return d

    def save(self, path: str):
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)
