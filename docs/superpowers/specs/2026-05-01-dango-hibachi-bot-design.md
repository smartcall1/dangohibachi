# Dango-Hibachi 델타 뉴트럴 거래량 봇 설계

**날짜**: 2026-05-01  
**목적**: Dango(maker) + Hibachi(taker) XEMM 패턴으로 자산 손실 최소화 + 거래량 극대화  
**프로젝트 경로**: `D:\Codes\dango_hibachi_bot`

---

## 1. 목표

- **1순위**: 최대한 많은 거래량 생성
- **2순위**: 포트폴리오 자산 손실 최소화
- 펀딩레이트 차익보다 **빠른 사이클 회전**이 핵심
- ETH / BTC 두 페어 중 자동으로 유리한 페어 선택

---

## 2. 거래소 역할 및 수수료

| 거래소 | 역할 | Maker 수수료 | Taker 수수료 |
|--------|------|-------------|-------------|
| **Dango** | Maker (지정가 주문) | 0% | 0.1% |
| **Hibachi** | Taker (즉시 체결) | 0% | ~0.045% |

**왕복 수수료**: 0% (Dango maker) + 0.045% (Hibachi taker) × 2 = **0.09%**  
(기존 Hibachi maker + Dango taker 방식 0.2% 대비 절반 이하)

---

## 3. 전체 아키텍처

### 3.1 파일 구조

```
D:\Codes\dango_hibachi_bot\
├── main.py                     # Watchdog (크래시 자동 재시작)
├── engine.py                   # 핵심 상태머신 엔진
├── strategy.py                 # 방향 결정, 청산 조건 함수
├── models.py                   # BotState, Position, Cycle 데이터 클래스
├── config.py                   # 환경변수 기반 설정
├── monitor.py                  # 마진/Circuit Breaker 모니터링
├── telegram_ui.py              # 텔레그램 UI 및 알림
├── exchanges/
│   ├── dango_client.py         # Dango GraphQL + EIP-712 클라이언트 (신규)
│   └── hibachi_client.py       # delta_donemoji_bot에서 재활용
├── logs/
│   ├── bot_state.json          # 상태 영속화
│   ├── cycles.jsonl            # 완료된 사이클 기록
│   ├── spread_history.jsonl    # HOLD 중 MTM 기록
│   └── engine.log
└── .env                        # 인증 정보
```

### 3.2 재활용 vs 신규

| 파일 | 출처 | 비고 |
|------|------|------|
| `exchanges/hibachi_client.py` | delta_donemoji_bot | 그대로 복사 |
| `monitor.py` | nado_grvt_bot | 소폭 수정 |
| `models.py` | nado_grvt_bot | 적응형 수정 |
| `telegram_ui.py` | nado_grvt_bot | 소폭 수정 |
| `main.py` (watchdog) | nado_grvt_bot | 그대로 복사 |
| `exchanges/dango_client.py` | — | **완전 신규** |
| `engine.py` | — | **신규** (두 봇 패턴 혼합) |
| `strategy.py` | — | **신규** (거래량 우선 로직) |
| `config.py` | — | **신규** |

---

## 4. 상태머신

```
IDLE → ANALYZE → ENTER → HOLD → EXIT → COOLDOWN → IDLE
                              ↓
                    HOLD_SUSPENDED (Dango API 장애)
                              ↓
                   MANUAL_INTERVENTION (청산 실패 3회)
```

| 상태 | 설명 |
|------|------|
| **IDLE** | 페어 선택 대기, 5초마다 ETH/BTC 펀딩레이트 스캔 |
| **ANALYZE** | 선택 페어 방향 결정, 잔고 확인 |
| **ENTER** | Dango-first XEMM 청크 5개 진입 |
| **HOLD** | 포지션 보유, 30초마다 청산 조건 평가 |
| **HOLD_SUSPENDED** | Dango 서버 점검/장애 시 대기 |
| **EXIT** | Dango-first XEMM 청크 5개 청산 |
| **COOLDOWN** | 5분 대기 후 IDLE 복귀 |
| **MANUAL_INTERVENTION** | 청산 3회 실패 시 수동 개입 요청 |

---

## 5. Dango 클라이언트 설계

### 5.1 인증

```
# .env
DANGO_PRIVATE_KEY=0x...        # MetaMask EVM 개인키
DANGO_ACCOUNT_ADDRESS=0x...    # 지갑 주소
DANGO_PERPS_CONTRACT=...       # Perps 컨트랙트 주소

# 서명 흐름
SignDoc 구성 → SHA-256 해싱 → web3.py Secp256k1 서명 → GraphQL mutation 전송
```

### 5.2 REST (GraphQL POST)

- **Mainnet**: `https://api-mainnet.dango.zone/graphql`
- **Testnet**: `https://api-testnet.dango.zone/graphql`

| 메서드 | 용도 |
|--------|------|
| `get_orderbook(symbol)` | BBO 조회 |
| `get_funding_rate(symbol)` | 펀딩레이트 조회 |
| `get_position(symbol)` | 포지션 조회 |
| `get_balance()` | 계좌 잔고 |
| `place_limit_order(symbol, side, price, size)` | Maker 주문 (post-only) |
| `cancel_order(order_id)` | 주문 취소 |
| `get_mark_price(symbol)` | 마크 가격 |

### 5.3 WebSocket (체결 이벤트)

```
wss://api-mainnet.dango.zone/graphql

구독: order_filled 이벤트
  수신 필드: order_id, filled_size, fill_price, fee
  체결 감지 → 즉시 Hibachi taker 트리거 콜백 호출
```

---

## 6. Dango-first XEMM 청크 로직

### 6.1 진입 (Dango maker → Hibachi taker)

```
for chunk_idx in range(5):

  [Phase 1 — Maker 무제한 재시도]
  anchor_price = Dango BBO 재조회
  concession = $0.00

  while True:
    price = anchor_price ± concession  (LONG: ask-concession, SHORT: bid+concession)
    order_id = Dango.place_limit_order(price, chunk_size)
    wait WebSocket fill event (timeout: 60초)

    if 체결됨:
      hb_actual = Hibachi.close_position(filled_size, taker)  # 즉시 실행
      break

    # 미체결 → 취소 후 재시도
    Dango.cancel_order(order_id)
    concession += $0.01

    if concession > spread × 0.5:
      # BBO 재앵커링 (시장 이동 대응)
      anchor_price = Dango BBO 재조회
      concession = $0.00
      continue  # 무한 재시도

  [긴급 Taker Fallback — 2가지 조건만]
  예외 1: Hibachi 마진 ≤ 10%  → Dango taker 강제 집행
  예외 2: Dango 장애 중 + Hibachi 마진 ≤ 10% → Hibachi만 먼저 청산 + 텔레그램 경보
```

### 6.2 청산 (동일 패턴, reduce_only 플래그)

진입과 동일한 Dango-first XEMM 패턴.  
Dango 주문에 `reduce_only: true` 플래그 추가.

---

## 7. 거래량 우선 전략

### 7.1 방향 결정

```python
net_A = dango_funding_8h - hibachi_funding_8h   # Dango LONG + Hibachi SHORT
net_B = hibachi_funding_8h - dango_funding_8h   # Dango SHORT + Hibachi LONG

direction = "A" if net_A >= net_B else "B"
# 둘 다 음수여도 진입 (거래량 봇 — 덜 손해인 방향 선택)
```

### 7.2 청산 조건 (우선순위 순)

| 우선순위 | 조건 | 비고 |
|---------|------|------|
| 1 | Hibachi 마진 ≤ 10% | 즉시 청산 (긴급) |
| 2 | 잔고 ≥ 진입 전 잔고 + 왕복 수수료 + $10 버퍼 | 원금 회수 (핵심 트리거) |
| 3 | 스프레드 MTM ≥ $30 | 기회적 익절 |
| 4 | 보유 기간 ≥ 4일 | 강제 청산 |
| 5 | 펀딩 방향 역전 + MIN_HOLD 경과 | 손해 확대 전 탈출 |

### 7.3 페어 선택 (ETH / BTC)

```python
# 매 IDLE 진입 시
for pair in ["ETH", "BTC"]:
    score[pair] = abs(net_funding[pair])  # 펀딩 스프레드 절댓값
    # 추후: 유동성 부족 시 페널티 적용 가능

best_pair = max(score, key=score.get)
```

### 7.4 핵심 파라미터

```
# 거래 설정
LEVERAGE = 3
PAIRS = ["ETH", "BTC"]
ENTRY_CHUNKS = 5
EXIT_CHUNKS = 5

# 보유 시간
MIN_HOLD_MINUTES = 30
MAX_HOLD_DAYS = 4
COOLDOWN_MINUTES = 5

# 청산 임계값
MARGIN_EMERGENCY_PCT = 10
MARGIN_WARNING_PCT = 15
SPREAD_OPPORTUNISTIC_USD = 30
PRINCIPAL_BUFFER_USD = 10

# XEMM 실행
MAKER_FILL_TIMEOUT_SECONDS = 60
MAKER_PRICE_STEP_USD = 0.01
EMERGENCY_CLOSE_SLIPPAGE_PCT = 0.05

# 수수료 (왕복)
DANGO_MAKER_FEE = 0.0
HIBACHI_TAKER_FEE = 0.00045
ROUND_TRIP_FEE_RATE = 0.0009  # 0.09%
```

---

## 8. API 장애 대응 (HOLD_SUSPENDED)

Dango는 신생 거래소로 서버 점검이 잦음 (1시간 이상 다운 가능).

| 시나리오 | 대응 |
|---------|------|
| ENTER 중 Dango 다운 | 진행 중 청크 취소, IDLE 복귀 대기 |
| HOLD 중 Dango 다운 | HOLD_SUSPENDED 진입, 복구 대기 (시간 제한 없음) |
| EXIT 중 Dango 다운 | Dango 복구 대기, 재시도 |
| Dango 다운 + Hibachi 마진 ≤ 10% | Hibachi만 먼저 청산 + 텔레그램 "Dango 수동 청산 필요" 경보 |
| MANUAL_INTERVENTION | EXIT 3회 실패 시, 30분마다 텔레그램 리마인더 |

---

## 9. 수익 구조 분석

$10,000 notional, 레버리지 3배 기준:

| 항목 | 금액 |
|------|------|
| 왕복 수수료 | $9 (0.09%) |
| 원금 회수 트리거 | 진입 잔고 + $19 (수수료 $9 + 버퍼 $10) |
| 펀딩 스프레드 0.01%/8h → 회수 시간 | ~19시간 |
| 펀딩 스프레드 0.05%/8h → 회수 시간 | ~3.8시간 |

목표: 펀딩 수령 없이도 스프레드 MTM만으로 원금 회수 가능한 사이클 반복.

---

## 10. 텔레그램 알림

| 이벤트 | 메시지 |
|--------|--------|
| 진입 완료 | `[ENTER] {pair} {direction} ${notional} @ {price}` |
| 청산 완료 | `[EXIT] {reason} PnL: ${pnl} 사이클 #{n}` |
| 마진 경고 | `[⚠️ MARGIN] {exchange} {ratio}%` |
| Dango 장애 | `[🔧 DANGO DOWN] HOLD_SUSPENDED 진입` |
| Dango 복구 | `[✅ DANGO UP] HOLD 재개` |
| 긴급 편측 청산 | `[🚨 HIBACHI CLOSED] Dango 수동 청산 필요! 포지션: {size}` |
| MANUAL | `[🚨 MANUAL] EXIT 실패 {n}회 — 수동 개입 필요` |

---

## 11. 환경변수 (.env)

```bash
# Dango
DANGO_PRIVATE_KEY=0x...
DANGO_ACCOUNT_ADDRESS=0x...
DANGO_PERPS_CONTRACT=...

# Hibachi (delta_donemoji_bot과 동일)
HIBACHI_API_KEY=...
HIBACHI_PRIVATE_KEY=0x...
HIBACHI_PUBLIC_KEY=...
HIBACHI_ACCOUNT_ID=...

# 텔레그램
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...

# 거래 설정 (선택적 오버라이드)
LEVERAGE=3
MIN_HOLD_MINUTES=30
PRINCIPAL_BUFFER_USD=10
```

---

## 12. 미결 사항 (구현 시 확인 필요)

1. **Dango GraphQL 스키마 확인**: `place_limit_order` mutation 정확한 필드명 및 서명 포맷
2. **Dango WebSocket 인증**: `order_filled` 구독에 서명 필요 여부
3. **Dango 논스 관리**: 슬라이딩 윈도우(±100) 동시 요청 시 충돌 방지
4. **Dango 지원 페어**: ETH-PERP, BTC-PERP 심볼명 확인
5. **Hibachi ETH/BTC 심볼**: `ETH/USDT-P`, `BTC/USDT-P` 확인
