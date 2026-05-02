"""워치독 진입점 — 크래시 자동 재시작"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import time

_LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(_LOG_DIR, "engine.log"), encoding="utf-8"),
    ],
)
logger = logging.getLogger("main")

RESTART_DELAY = 5
MAX_RESTARTS = 20


async def _run_once():
    from config import Config
    from exchanges.dango_client import DangoClient
    from exchanges.hibachi_client import HibachiClient
    from engine import Engine
    from telegram_ui import TelegramUI

    errors = Config.validate()
    if errors:
        for e in errors:
            logger.error("설정 오류: %s", e)
        sys.exit(1)

    Config.ensure_dirs()

    dango = DangoClient(
        private_key=Config.DANGO_PRIVATE_KEY,
        account_address=Config.DANGO_ACCOUNT_ADDRESS,
        perps_contract=Config.DANGO_PERPS_CONTRACT,
        chain_id=Config.DANGO_CHAIN_ID,
        gql_url=Config.DANGO_GQL_URL,
        ws_url=Config.DANGO_WS_URL,
    )
    hibachi = HibachiClient(
        api_key=Config.HIBACHI_API_KEY,
        private_key=Config.HIBACHI_PRIVATE_KEY,
        public_key=Config.HIBACHI_PUBLIC_KEY,
        account_id=Config.HIBACHI_ACCOUNT_ID,
    )
    tg = TelegramUI(Config.TELEGRAM_BOT_TOKEN, Config.TELEGRAM_CHAT_ID)
    engine = Engine(dango, hibachi, tg)
    tg.set_engine(engine)

    await tg.start()
    await tg.send("🤖 Dango-Hibachi 봇 시작")
    await engine.run()


def main():
    restarts = 0

    while restarts < MAX_RESTARTS:
        try:
            logger.info("봇 시작 (재시작 #%d)", restarts)
            asyncio.run(_run_once())
            logger.info("봇 정상 종료")
            break
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt — 종료")
            break
        except Exception as e:
            restarts += 1
            logger.error("봇 크래시 (#%d): %s — %ds 후 재시작", restarts, e, RESTART_DELAY, exc_info=True)
            time.sleep(RESTART_DELAY)

    if restarts >= MAX_RESTARTS:
        logger.critical("최대 재시작 횟수(%d) 초과 — 수동 확인 필요", MAX_RESTARTS)


if __name__ == "__main__":
    main()
