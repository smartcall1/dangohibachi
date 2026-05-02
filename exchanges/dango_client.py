"""Dango Perps GraphQL 클라이언트 — REST + WebSocket + EIP-712 서명"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import time
import uuid
from typing import Any, Callable, Optional

import httpx
import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)

# graphql-ws 프로토콜 메시지 타입
_GQL_CONNECTION_INIT = "connection_init"
_GQL_CONNECTION_ACK = "connection_ack"
_GQL_SUBSCRIBE = "subscribe"
_GQL_NEXT = "next"
_GQL_ERROR = "error"
_GQL_COMPLETE = "complete"

_GQL_WS_SUBPROTOCOL = "graphql-transport-ws"


def _canonical_json(obj: Any) -> str:
    """재귀적 알파벳 정렬 canonical JSON (Dango SignDoc 서명용)"""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _to_gql_literal(obj: Any) -> str:
    """Python dict/list/str → GraphQL object literal 문자열 (키 따옴표 없음)"""
    if isinstance(obj, dict):
        parts = [f"{k}:{_to_gql_literal(v)}" for k, v in obj.items()]
        return "{" + ",".join(parts) + "}"
    elif isinstance(obj, list):
        return "[" + ",".join(_to_gql_literal(i) for i in obj) + "]"
    elif isinstance(obj, str):
        return json.dumps(obj)
    elif obj is None:
        return "null"
    elif isinstance(obj, bool):
        return "true" if obj else "false"
    else:
        return str(obj)


def _sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _sign_secp256k1(hash_bytes: bytes, private_key_hex: str) -> str:
    """secp256k1 raw sign → 64바이트 base64 (r+s, no recovery id)"""
    from eth_keys import keys as eth_keys

    pk_bytes = bytes.fromhex(private_key_hex.lstrip("0x"))
    pk = eth_keys.PrivateKey(pk_bytes)
    sig = pk.sign_msg_hash(hash_bytes)
    raw = sig.r.to_bytes(32, "big") + sig.s.to_bytes(32, "big")
    return base64.b64encode(raw).decode()


def _derive_key_hash(private_key_hex: str) -> str:
    """SHA-256(압축 공개키) → 64자 hex (Dango credential key_hash)"""
    from eth_keys import keys as eth_keys

    pk_bytes = bytes.fromhex(private_key_hex.lstrip("0x"))
    pk = eth_keys.PrivateKey(pk_bytes)
    # 압축 공개키: 33바이트
    pub_uncompressed = pk.public_key.to_bytes()  # 64바이트 (prefix 없음)
    # x 좌표로 압축 공개키 구성
    x = pub_uncompressed[:32]
    y_last_byte = pub_uncompressed[63]
    prefix = b"\x02" if (y_last_byte % 2 == 0) else b"\x03"
    compressed = prefix + x
    return hashlib.sha256(compressed).hexdigest()


class DangoClient:
    """
    Dango Perps REST + WebSocket 클라이언트.

    사용법:
        client = DangoClient(private_key, account_address, perps_contract, chain_id, gql_url, ws_url)
        await client.start()        # WebSocket 이벤트 구독 시작
        bbo = await client.get_bbo("perp/ethusd")
        order_id = await client.place_limit_order(...)
        await client.cancel_order_by_client_id("perp/ethusd", cid)
        await client.stop()
    """

    def __init__(
        self,
        private_key: str,
        account_address: str,
        perps_contract: str,
        chain_id: str,
        gql_url: str,
        ws_url: str,
    ):
        self._pk = private_key
        self._addr = account_address.lower()
        self._contract = perps_contract
        self._chain_id = chain_id
        self._gql_url = gql_url
        self._ws_url = ws_url
        self._key_hash = _derive_key_hash(private_key)

        # 논스: timestamp 기반 (ms), 각 tx마다 증분
        self._nonce = int(time.time() * 1000)
        self._nonce_lock = asyncio.Lock()

        # WebSocket 이벤트 콜백: client_order_id → asyncio.Event + fill data
        self._fill_events: dict[str, asyncio.Event] = {}
        self._fill_data: dict[str, dict] = {}

        self._ws_task: Optional[asyncio.Task] = None
        self._running = False
        self._http = httpx.AsyncClient(timeout=15.0)

    # ──────────────────────────────────────────────
    # 논스 관리
    # ──────────────────────────────────────────────

    async def _next_nonce(self) -> int:
        async with self._nonce_lock:
            self._nonce += 1
            return self._nonce

    # ──────────────────────────────────────────────
    # 서명 & 트랜잭션 구성
    # ──────────────────────────────────────────────

    def _build_tx(self, msg: dict, nonce: int, gas_limit: int = 2_000_000) -> dict:
        sign_doc = {
            "chain_id": self._chain_id,
            "expiry": None,
            "gas_limit": gas_limit,
            "messages": [{"execute": {"contract": self._contract, "funds": {}, "msg": msg}}],
            "nonce": nonce,
            "sender": self._addr,
        }
        canonical = _canonical_json(sign_doc)
        hash_bytes = _sha256(canonical.encode())
        sig_b64 = _sign_secp256k1(hash_bytes, self._pk)
        return {
            "sender": self._addr,
            "gas_limit": gas_limit,
            "msgs": [{"execute": {"contract": self._contract, "funds": {}, "msg": msg}}],
            "data": {
                "chain_id": self._chain_id,
                "expiry": None,
                "nonce": nonce,
                "user_index": 0,
            },
            "credential": {
                "standard": {
                    "key_hash": self._key_hash,
                    "signature": {"secp256k1": sig_b64},
                }
            },
        }

    async def _broadcast(self, msg: dict) -> dict:
        """트랜잭션을 broadcastTxSync로 전송"""
        nonce = await self._next_nonce()
        tx = self._build_tx(msg, nonce)
        query = """
        mutation BroadcastTx($tx: Tx!) {
          broadcastTxSync(tx: $tx)
        }
        """
        resp = await self._http.post(
            self._gql_url,
            json={"query": query, "variables": {"tx": tx}},
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        data = resp.json()
        if "errors" in data:
            raise RuntimeError(f"Dango broadcast error: {data['errors']}")
        return data.get("data", {}).get("broadcastTxSync", {})

    # ──────────────────────────────────────────────
    # REST 조회 헬퍼
    # ──────────────────────────────────────────────

    async def _query_app(self, msg: dict) -> Any:
        # GrugQueryInput은 GraphQL object literal 방식으로만 동작함 (JSON string 변수 불가)
        msg_literal = _to_gql_literal(msg)
        query = (
            "{queryApp(request:{wasm_smart:{contract:"
            + json.dumps(self._contract)
            + ",msg:"
            + msg_literal
            + "}})}  "
        )
        resp = await self._http.post(
            self._gql_url,
            json={"query": query},
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        data = resp.json()
        if "errors" in data:
            raise RuntimeError(f"Dango queryApp error: {data['errors']}")
        # 응답 구조: data.queryApp.wasm_smart = {...실제 데이터...}
        result = data["data"]["queryApp"]
        return result["wasm_smart"] if result else None

    async def _query_pair_stats(self, pair_id: str) -> dict:
        query = """
        query PairStats($pairId: String!) {
          perpsPairStats(pairId: $pairId) {
            currentPrice
            price24HAgo
            volume24H
          }
        }
        """
        resp = await self._http.post(
            self._gql_url,
            json={"query": query, "variables": {"pairId": pair_id}},
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        data = resp.json()
        if "errors" in data:
            raise RuntimeError(f"Dango pairStats error: {data['errors']}")
        return data["data"]["perpsPairStats"] or {}

    # ──────────────────────────────────────────────
    # 공개 API — 시세/포지션/잔고
    # ──────────────────────────────────────────────

    async def get_bbo(self, pair_id: str) -> dict:
        """BBO (best bid/ask) 조회. {"bid": float, "ask": float, "mark": float}"""
        result = await self._query_app({
            "liquidity_depth": {
                "pair_id": pair_id,
                "direction": "bid",
                "start_price": None,
                "limit": 5,
                "bucket_size": "1.000000",
            }
        })
        # 응답: {bids: {price_str: {notional, size}}, asks: {price_str: {notional, size}}}
        bids = result.get("bids", {}) if result else {}
        asks = result.get("asks", {}) if result else {}
        best_bid = max((float(p) for p in bids), default=0.0)
        best_ask = min((float(p) for p in asks), default=0.0)

        try:
            stats = await self._query_pair_stats(pair_id)
            mark = float(stats.get("currentPrice", 0) or 0)
        except Exception:
            mark = (best_bid + best_ask) / 2 if best_bid and best_ask else 0.0

        return {"bid": best_bid, "ask": best_ask, "mark": mark}

    async def get_mark_price(self, pair_id: str) -> float:
        bbo = await self.get_bbo(pair_id)
        return bbo["mark"]

    async def get_funding_rate(self, pair_id: str) -> float:
        """현재 펀딩레이트 (per 8h 환산). 양수 = LONG이 SHORT에 지급."""
        result = await self._query_app({"pair_state": {"pair_id": pair_id}})
        if not result:
            return 0.0
        # pair_state 응답: {funding_rate, funding_per_unit, long_oi, short_oi}
        # funding_rate 단위: 실측 기준 ~0.000022 (정확한 주기 미확인 → 8h 기준으로 사용)
        return float(result.get("funding_rate", 0) or 0)

    async def _query_user_state(self) -> Optional[dict]:
        """user_state 조회. 계정 미존재 시 None 반환.

        실제 응답: {margin, reserved_margin, positions, open_order_count, vault_shares, unlocks}
        """
        try:
            return await self._query_app({"user_state": {"user": self._addr}})
        except RuntimeError as e:
            if "data not found" in str(e):
                return None
            raise

    async def get_position(self, pair_id: str) -> Optional[dict]:
        """포지션 조회. 포지션 없으면 None."""
        result = await self._query_user_state()
        if not result:
            return None
        positions = result.get("positions", {})
        return positions.get(pair_id)

    async def get_balance(self) -> dict:
        """계좌 잔고 조회. {"equity": float, "margin": float, "available_margin": float}

        실제 응답 필드: margin, reserved_margin (equity/available_margin 필드 없음)
        available = margin - reserved_margin
        """
        result = await self._query_user_state()
        if not result:
            return {"equity": 0.0, "margin": 0.0, "available_margin": 0.0}
        margin = float(result.get("margin", 0) or 0)
        reserved = float(result.get("reserved_margin", 0) or 0)
        available = margin - reserved
        return {"equity": margin, "margin": margin, "available_margin": available}

    # ──────────────────────────────────────────────
    # 주문 실행
    # ──────────────────────────────────────────────

    async def place_limit_order(
        self,
        pair_id: str,
        side: str,
        price: float,
        size: float,
        reduce_only: bool = False,
        post_only: bool = True,
        client_order_id: Optional[str] = None,
    ) -> str:
        """Maker 지정가 주문 전송. client_order_id 반환."""
        cid = client_order_id or str(uuid.uuid4())[:16]
        # Dango: size는 LONG이면 양수, SHORT면 음수
        signed_size = size if side.upper() == "BUY" else -size
        tif = "POST" if post_only else "GTC"

        msg = {
            "trade": {
                "submit_order": {
                    "pair_id": pair_id,
                    "size": f"{signed_size:.6f}",
                    "kind": {
                        "limit": {
                            "limit_price": f"{price:.6f}",
                            "time_in_force": tif,
                            "client_order_id": cid,
                        }
                    },
                    "reduce_only": reduce_only,
                }
            }
        }
        await self._broadcast(msg)
        logger.info("Dango limit order placed: %s %s %s@%.4f cid=%s", pair_id, side, size, price, cid)
        return cid

    async def cancel_order_by_client_id(self, pair_id: str, client_order_id: str) -> dict:
        """client_order_id로 주문 취소"""
        msg = {
            "trade": {
                "cancel_order": {
                    "pair_id": pair_id,
                    "order_id": {"one_by_client_order_id": client_order_id},
                }
            }
        }
        try:
            result = await self._broadcast(msg)
            logger.info("Dango order cancelled: cid=%s", client_order_id)
            return result or {}
        except Exception as e:
            logger.warning("Dango cancel error (cid=%s): %s", client_order_id, e)
            return {}

    async def cancel_all_orders(self, pair_id: str) -> dict:
        """페어 전체 주문 취소"""
        msg = {"trade": {"cancel_order": {"pair_id": pair_id, "order_id": "all"}}}
        try:
            return await self._broadcast(msg) or {}
        except Exception as e:
            logger.warning("Dango cancel_all error: %s", e)
            return {}

    async def place_market_order(
        self, pair_id: str, side: str, size: float, slippage: float = 0.05
    ) -> dict:
        """긴급 taker 시장가 주문 (fallback용)"""
        signed_size = size if side.upper() == "BUY" else -size
        msg = {
            "trade": {
                "submit_order": {
                    "pair_id": pair_id,
                    "size": f"{signed_size:.6f}",
                    "kind": {"market": {"max_slippage": f"{slippage:.6f}"}},
                    "reduce_only": True,
                }
            }
        }
        result = await self._broadcast(msg)
        logger.info("Dango market order: %s %s %s slippage=%.2f%%", pair_id, side, size, slippage * 100)
        return result or {}

    # ──────────────────────────────────────────────
    # WebSocket — order_filled 이벤트 구독
    # ──────────────────────────────────────────────

    async def wait_for_fill(self, client_order_id: str, timeout: float) -> Optional[dict]:
        """client_order_id 체결 이벤트 대기. timeout 초 초과 시 None 반환."""
        event = asyncio.Event()
        self._fill_events[client_order_id] = event
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            return self._fill_data.pop(client_order_id, None)
        except asyncio.TimeoutError:
            return None
        finally:
            self._fill_events.pop(client_order_id, None)

    def _on_fill_event(self, event_data: dict):
        """WebSocket에서 order_filled 이벤트 수신 시 콜백"""
        data = event_data.get("data", {})
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception:
                return

        # client_order_id 또는 order_id로 매핑 시도
        cid = str(data.get("client_order_id", ""))
        order_id = str(data.get("order_id", ""))

        for key in (cid, order_id):
            if key and key in self._fill_events:
                self._fill_data[key] = data
                self._fill_events[key].set()
                logger.info("Fill received: cid=%s size=%s price=%s", key,
                            data.get("fill_size"), data.get("fill_price"))
                break

    async def _ws_loop(self):
        """graphql-ws 프로토콜로 order_filled 이벤트 구독"""
        retry_delay = 2
        subscription_query = """
        subscription OrderFills($userAddr: String!) {
          events(
            filter: [
              {
                type: "order_filled"
                data: [{ path: ["user"], checkMode: EQUAL, value: [$userAddr] }]
              }
            ]
          ) {
            type
            data
          }
        }
        """
        while self._running:
            try:
                    async with websockets.connect(
                        self._ws_url,
                        subprotocols=[_GQL_WS_SUBPROTOCOL],
                        ping_interval=20,
                        ping_timeout=20,
                    ) as ws:
                    # 연결 초기화
                    await ws.send(json.dumps({"type": _GQL_CONNECTION_INIT, "payload": {}}))
                    ack = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
                    if ack.get("type") != _GQL_CONNECTION_ACK:
                        raise RuntimeError(f"WS connection_ack 실패: {ack}")

                    # 구독 등록
                    await ws.send(json.dumps({
                        "id": "fill_sub",
                        "type": _GQL_SUBSCRIBE,
                        "payload": {
                            "query": subscription_query,
                            "variables": {"userAddr": self._addr},
                        },
                    }))
                    logger.info("Dango WS 구독 시작 (order_filled, user=%s)", self._addr)
                    retry_delay = 2

                    async for raw in ws:
                        msg = json.loads(raw)
                        msg_type = msg.get("type")
                        if msg_type == _GQL_NEXT:
                            payload = msg.get("payload", {})
                            events_data = payload.get("data", {}).get("events")
                            if events_data and events_data.get("type") == "order_filled":
                                self._on_fill_event(events_data)
                        elif msg_type == _GQL_ERROR:
                            logger.error("Dango WS 구독 에러: %s", msg.get("payload"))
                        elif msg_type == _GQL_COMPLETE:
                            logger.warning("Dango WS 구독 종료됨")
                            break

            except ConnectionClosed as e:
                if not self._running:
                    break
                logger.warning("Dango WS 연결 끊김, %ds 후 재연결: %s", retry_delay, e)
            except Exception as e:
                if not self._running:
                    break
                logger.warning("Dango WS 오류, %ds 후 재연결: %s", retry_delay, e)

            if self._running:
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)

    async def start(self):
        """WebSocket 이벤트 구독 시작"""
        self._running = True
        self._ws_task = asyncio.create_task(self._ws_loop())

    async def stop(self):
        """클라이언트 종료"""
        self._running = False
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        await self._http.aclose()

    # ──────────────────────────────────────────────
    # 헬스체크
    # ──────────────────────────────────────────────

    async def is_healthy(self) -> bool:
        """API 응답 여부 확인 (1초 타임아웃)"""
        try:
            async with self._http.stream("POST", self._gql_url,
                                          json={"query": "{ __typename }"},
                                          headers={"Content-Type": "application/json"},
                                          timeout=3.0) as r:
                return r.status_code < 500
        except Exception:
            return False
