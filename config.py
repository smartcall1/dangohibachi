"""환경변수 기반 설정"""
import os
from dotenv import load_dotenv

_ROOT = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_ROOT, ".env"))


class Config:
    # Dango
    DANGO_PRIVATE_KEY: str = os.getenv("DANGO_PRIVATE_KEY", "")
    DANGO_ACCOUNT_ADDRESS: str = os.getenv("DANGO_ACCOUNT_ADDRESS", "")
    DANGO_PERPS_CONTRACT: str = os.getenv(
        "DANGO_PERPS_CONTRACT", "0x90bc84df68d1aa59a857e04ed529e9a26edbea4f"
    )
    DANGO_CHAIN_ID: str = os.getenv("DANGO_CHAIN_ID", "dango-1")
    DANGO_GQL_URL: str = "https://api-mainnet.dango.zone/graphql"
    DANGO_WS_URL: str = "wss://api-mainnet.dango.zone/graphql"

    # Hibachi
    HIBACHI_API_KEY: str = os.getenv("HIBACHI_API_KEY", "")
    HIBACHI_PRIVATE_KEY: str = os.getenv("HIBACHI_PRIVATE_KEY", "")
    HIBACHI_PUBLIC_KEY: str = os.getenv("HIBACHI_PUBLIC_KEY", "")
    HIBACHI_ACCOUNT_ID: str = os.getenv("HIBACHI_ACCOUNT_ID", "")

    # 거래 설정
    LEVERAGE: int = int(os.getenv("LEVERAGE", "3"))
    # 활성 페어. Dango 측 유동성 부재로 현재 BTC만 사용.
    # ETH/SOL/HYPE 유동성 회복되면 .env에 PAIRS=ETH,BTC,SOL,HYPE 로 오버라이드 가능.
    PAIRS: list = os.getenv("PAIRS", "BTC").split(",")
    # Dango 유동성이 얇아 큰 청크는 체결률 낮음 — 10개로 분할 (env 오버라이드 가능)
    ENTRY_CHUNKS: int = int(os.getenv("ENTRY_CHUNKS", "10"))
    EXIT_CHUNKS: int = int(os.getenv("EXIT_CHUNKS", "10"))

    # 심볼 매핑
    DANGO_SYMBOL_MAP: dict = {
        "ETH": "perp/ethusd",
        "BTC": "perp/btcusd",
        "SOL": "perp/solusd",
        "HYPE": "perp/hypeusd",
    }
    HIBACHI_SYMBOL_MAP: dict = {
        "ETH": "ETH/USDT-P",
        "BTC": "BTC/USDT-P",
        "SOL": "SOL/USDT-P",
        "HYPE": "HYPE/USDT-P",
    }
    # Dango 페어별 tick size (limit price는 이 배수만 허용 — 검증된 값)
    # BTC: 0.1 / 다른 페어는 실제 거래 시 chain error로 확인 후 보정
    PAIR_TICK_SIZE: dict = {
        "BTC": 0.1,
        "ETH": 0.01,
        "SOL": 0.001,
        "HYPE": 0.001,
    }
    # Hibachi 페어별 tick size (BTC 0.1 — 2026-05-03 mainnet 거부 로그로 확인)
    HIBACHI_TICK_SIZE: dict = {
        "BTC": 0.1,
        "ETH": 0.01,
        "SOL": 0.001,
        "HYPE": 0.001,
    }

    # 보유 시간
    MIN_HOLD_MINUTES: int = int(os.getenv("MIN_HOLD_MINUTES", "30"))
    MAX_HOLD_DAYS: int = int(os.getenv("MAX_HOLD_DAYS", "4"))
    COOLDOWN_MINUTES: int = int(os.getenv("COOLDOWN_MINUTES", "5"))

    # 청산 임계값
    MARGIN_EMERGENCY_PCT: float = float(os.getenv("MARGIN_EMERGENCY_PCT", "10"))
    MARGIN_WARNING_PCT: float = float(os.getenv("MARGIN_WARNING_PCT", "15"))
    SPREAD_OPPORTUNISTIC_USD: float = float(os.getenv("SPREAD_OPPORTUNISTIC_USD", "30"))
    PRINCIPAL_BUFFER_USD: float = float(os.getenv("PRINCIPAL_BUFFER_USD", "10"))

    # XEMM 실행
    # Dango 얇은 책 — 체결되면 수초, 안 되면 영영 안 됨. 길게 잡을 의미 없음.
    MAKER_FILL_TIMEOUT_SECONDS: int = int(os.getenv("MAKER_FILL_TIMEOUT_SECONDS", "15"))
    MAKER_PRICE_STEP_USD: float = float(os.getenv("MAKER_PRICE_STEP_USD", "0.01"))
    # 청크당 maker 재시도 한도 — 무한 루프 방지 (delta_donemoji XEMM 패턴)
    MAKER_RETRY_LIMIT: int = int(os.getenv("MAKER_RETRY_LIMIT", "5"))
    EMERGENCY_CLOSE_SLIPPAGE_PCT: float = float(os.getenv("EMERGENCY_CLOSE_SLIPPAGE_PCT", "0.05"))
    MAX_EXIT_FAILURES: int = int(os.getenv("MAX_EXIT_FAILURES", "3"))
    # dust 임계 (notional USD) — 양쪽 합 미만이면 외부 정리/청산 완료로 간주, IDLE 전환
    # nado_grvt ed32123 / "5청크 후 dust 갇힘" 회귀 방지
    DUST_NOTIONAL_USD: float = float(os.getenv("DUST_NOTIONAL_USD", "10"))

    # 수수료 (왕복)
    DANGO_MAKER_FEE: float = 0.0
    HIBACHI_TAKER_FEE: float = 0.00045
    ROUND_TRIP_FEE_RATE: float = 0.0009  # 0.09%

    # 텔레그램
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # 폴링
    POLL_FUNDING_SECONDS: int = int(os.getenv("POLL_FUNDING_SECONDS", "300"))

    LOG_DIR: str = os.path.join(_ROOT, "logs")

    @classmethod
    def ensure_dirs(cls):
        os.makedirs(cls.LOG_DIR, exist_ok=True)

    @classmethod
    def validate(cls) -> list[str]:
        errors = []
        required = {
            "DANGO_PRIVATE_KEY": cls.DANGO_PRIVATE_KEY,
            "DANGO_ACCOUNT_ADDRESS": cls.DANGO_ACCOUNT_ADDRESS,
            "HIBACHI_API_KEY": cls.HIBACHI_API_KEY,
            "HIBACHI_PRIVATE_KEY": cls.HIBACHI_PRIVATE_KEY,
            "HIBACHI_PUBLIC_KEY": cls.HIBACHI_PUBLIC_KEY,
            "HIBACHI_ACCOUNT_ID": cls.HIBACHI_ACCOUNT_ID,
            "TELEGRAM_BOT_TOKEN": cls.TELEGRAM_BOT_TOKEN,
            "TELEGRAM_CHAT_ID": cls.TELEGRAM_CHAT_ID,
        }
        for name, val in required.items():
            if not val:
                errors.append(f"{name} 미설정")
        return errors
