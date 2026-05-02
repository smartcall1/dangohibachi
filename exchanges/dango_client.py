"""Dango Perps GraphQL 클라이언트 — REST + WebSocket + EIP-712 서명"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import uuid
from typing import Any, Optional

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
_GQL_PING = "ping"
_GQL_PONG = "pong"

_GQL_WS_SUBPROTOCOL = "graphql-transport-ws"
_GQL_KEEPALIVE_INTERVAL = 15  # GraphQL ping 주기 — Dango 30s 타임아웃 회피용


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


def _sign_raw(hash_bytes: bytes, private_key_hex: str) -> str:
    """secp256k1 raw sign → 64바이트 base64 (r+s, no recovery id)"""
    from eth_keys import keys as eth_keys

    pk_bytes = bytes.fromhex(private_key_hex.lstrip("0x"))
    pk = eth_keys.PrivateKey(pk_bytes)
    sig = pk.sign_msg_hash(hash_bytes)
    raw = sig.r.to_bytes(32, "big") + sig.s.to_bytes(32, "big")
    return base64.b64encode(raw).decode()


def _build_msg_types(type_name: str, value: Any, types_acc: dict) -> Optional[str]:
    """message 값에서 EIP-712 struct 타입을 재귀적으로 추론한다.
    types_acc에 타입 정의 누적, 반환값 = EIP-712 타입 이름 (primitive이면 string/bool/uint32)."""
    _ADDR_KEYS = frozenset({"contract", "sender", "verifyingContract"})
    _BOOL_KEYS = frozenset({"reduce_only"})
    _UINT32_KEYS = frozenset({"user_index", "nonce", "gas_limit"})

    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "uint32"
    if isinstance(value, str):
        return "string"
    if value is None:
        return None
    if isinstance(value, list):
        if not value:
            types_acc.setdefault(type_name, [])
            return f"{type_name}[]"
        elem_type = _build_msg_types(type_name, value[0], types_acc)
        return f"{elem_type}[]"
    if isinstance(value, dict):
        fields = []
        for k, v in value.items():
            if k in _ADDR_KEYS:
                ftype: Optional[str] = "address"
            elif k in _BOOL_KEYS or isinstance(v, bool):
                ftype = "bool"
            elif k in _UINT32_KEYS or (isinstance(v, int) and not isinstance(v, bool)):
                ftype = "uint32"
            elif isinstance(v, (dict, list)):
                child = type_name + "".join(p.capitalize() for p in k.split("_"))
                ftype = _build_msg_types(child, v, types_acc)
            else:
                ftype = "string"
            if ftype is not None:
                fields.append({"name": k, "type": ftype})
        types_acc[type_name] = fields
        return type_name
    return "string"


def _derive_key_hash_ethereum(private_key_hex: str) -> str:
    """SHA-256('0x' + lowercase_eth_address UTF-8) → 대문자 hex (ethereum 키 타입)"""
    from eth_hash.auto import keccak
    from eth_keys import keys as eth_keys

    pk_bytes = bytes.fromhex(private_key_hex.lstrip("0x"))
    pk = eth_keys.PrivateKey(pk_bytes)
    pub = pk.public_key.to_bytes()  # 64B
    eth_addr = "0x" + keccak(pub)[-20:].hex()  # EIP-55 없이 소문자
    return hashlib.sha256(eth_addr.encode("utf-8")).hexdigest().upper()


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

        # key_hash + 서명 타입 — start()에서 factory 조회로 초기화됨
        self._key_hash: str = ""
        self._key_type: str = "secp256k1"  # "secp256k1" | "ethereum"

        # Dango nonce는 순차 증분 — 계정 최초 논스 4부터 시작, 재시작 시 에러 자동 보정
        self._nonce: int = 4
        self._nonce_lock = asyncio.Lock()

        # Dango 계정 인덱스 (첫 주문 시 자동 탐색)
        self._user_index: int = int(os.environ.get("DANGO_USER_INDEX", "0"))
        self._user_index_found: bool = False

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

    def _extract_chain_nonce(self, err: str) -> Optional[int]:
        """에러 메시지에서 체인 nonce 파싱. 다음 _next_nonce 호출이 +1 하므로 N을 그대로 반환.
        - 'too far ahead: X > Y + ...' → Y (체인의 max seen)
        - 'already seen: N' → N (이미 사용된 nonce)
        """
        import re
        m = re.search(r"nonce is too far ahead: \d+ > (\d+) \+", err)
        if m:
            return int(m.group(1))
        m = re.search(r"nonce is already seen:\s*(\d+)", err)
        if m:
            return int(m.group(1))
        return None

    # ──────────────────────────────────────────────
    # 서명 & 트랜잭션 구성
    # ──────────────────────────────────────────────

    def _sign_eip712(self, inner_msg: dict, gas_limit: int, nonce: int, user_index: int) -> tuple:
        """EIP-712 TypedData 구성 + 서명. Returns (typed_data_b64, sig_b64)."""
        from eth_account import Account

        execute_msg = {"execute": {"contract": self._contract, "msg": inner_msg, "funds": {}}}
        metadata = {"user_index": user_index, "chain_id": self._chain_id, "nonce": nonce}
        message = {
            "sender": self._addr,
            "data": metadata,
            "gas_limit": gas_limit,
            "messages": [execute_msg],
        }
        domain = {"name": "dango", "chainId": 1, "verifyingContract": self._addr}

        # inner_msg 구조에서 타입 재귀 추론
        inner_types: dict = {}
        _build_msg_types("ExecuteMessage0", inner_msg, inner_types)

        msg_types = {
            "Metadata": [
                {"name": "user_index", "type": "uint32"},
                {"name": "chain_id", "type": "string"},
                {"name": "nonce", "type": "uint32"},
            ],
            "Message": [
                {"name": "sender", "type": "address"},
                {"name": "data", "type": "Metadata"},
                {"name": "gas_limit", "type": "uint32"},
                {"name": "messages", "type": "TxMessage[]"},
            ],
            "TxMessage": [{"name": "execute", "type": "Execute0"}],
            "Execute0": [
                {"name": "contract", "type": "address"},
                {"name": "msg", "type": "ExecuteMessage0"},
                {"name": "funds", "type": "Funds0"},
            ],
            "Funds0": [],
            **inner_types,
        }

        full_typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                **msg_types,
            },
            "primaryType": "Message",
            "domain": domain,
            "message": message,
        }

        pk_bytes = bytes.fromhex(self._pk.lstrip("0x"))
        acct = Account.from_key(pk_bytes)
        signed = acct.sign_typed_data(full_message=full_typed_data)

        # 65B: r(32)+s(32)+recovery_id(1). eth_account v=27/28 → 0/1 정규화
        sig_bytes = bytes(signed.signature)
        r_s = sig_bytes[:64]
        v = sig_bytes[64]
        recovery_id = v - 27 if v >= 27 else v
        sig_b64 = base64.b64encode(r_s + bytes([recovery_id])).decode()

        typed_data_b64 = base64.b64encode(
            json.dumps(full_typed_data, separators=(",", ":")).encode()
        ).decode()

        return typed_data_b64, sig_b64

    def _build_tx(self, msg: dict, nonce: int, user_index: int, gas_limit: int = 2_000_000) -> dict:
        execute_msgs = [{"execute": {"contract": self._contract, "funds": {}, "msg": msg}}]
        tx_data = {"chain_id": self._chain_id, "nonce": nonce, "user_index": user_index}

        if self._key_type == "ethereum":
            typed_data_b64, sig_b64 = self._sign_eip712(msg, gas_limit, nonce, user_index)
            signature = {"eip712": {"typed_data": typed_data_b64, "sig": sig_b64}}
        else:
            sign_doc = {
                "data": tx_data,
                "gas_limit": gas_limit,
                "messages": execute_msgs,
                "sender": self._addr,
            }
            sig_b64 = _sign_raw(_sha256(_canonical_json(sign_doc).encode()), self._pk)
            signature = {"secp256k1": sig_b64}

        return {
            "sender": self._addr,
            "gas_limit": gas_limit,
            "msgs": execute_msgs,
            "data": tx_data,
            "credential": {
                "standard": {
                    "key_hash": self._key_hash,
                    "signature": signature,
                }
            },
        }

    def _parse_broadcast_error(self, result: dict) -> Optional[str]:
        """broadcastTxSync 응답에서 에러 메시지 추출. 성공이면 None 반환."""
        check_tx = result.get("check_tx", {})
        tx_result = check_tx.get("result", {})
        if "Err" in tx_result:
            return tx_result["Err"].get("error", "unknown error")
        return None

    async def _verify_tx_committed(self, tx_hash: str, max_wait_s: float = 8.0) -> Optional[str]:
        """tx_hash가 블록에 포함되고 deliver_tx 성공인지 indexer로 확인.
        성공 시 None, 실패 시 에러 메시지 반환. 타임아웃 시 'verification timeout'."""
        if not tx_hash or tx_hash == "?":
            return None  # 검증 불가 — 기존 동작 유지
        query = (
            '{transactions(hash:"' + tx_hash +
            '", first:1){nodes{hasSucceeded errorMessage gasUsed}}}'
        )
        deadline = asyncio.get_event_loop().time() + max_wait_s
        delay = 0.4
        while asyncio.get_event_loop().time() < deadline:
            try:
                resp = await self._http.post(
                    self._gql_url,
                    json={"query": query},
                    headers={"Content-Type": "application/json"},
                )
                data = resp.json()
                nodes = (data.get("data") or {}).get("transactions", {}).get("nodes", [])
                if nodes:
                    node = nodes[0]
                    if node.get("hasSucceeded"):
                        return None
                    return node.get("errorMessage") or "deliver_tx failed (no error message)"
            except Exception:
                pass
            await asyncio.sleep(delay)
            delay = min(delay * 1.4, 1.5)
        return "verification timeout — tx not indexed within %.1fs" % max_wait_s

    async def _broadcast_once(self, msg: dict, user_index: int) -> dict:
        """단일 broadcastTxSync 전송 (user_index 지정)"""
        nonce = await self._next_nonce()
        tx = self._build_tx(msg, nonce, user_index)
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
            err_msg = f"Dango broadcast error: {data['errors']}"
            logger.error(err_msg)
            raise RuntimeError(err_msg)

        return data.get("data", {}).get("broadcastTxSync", {})

    async def _broadcast(self, msg: dict) -> dict:
        """트랜잭션 전송. user_index 불일치 + nonce 불일치 자동 보정."""
        max_scan = 5   # user_index 스캔 범위
        # Dango chain은 최근 20개 nonce를 기억. 봇 재시작 시 그 범위만큼 catch-up 필요.
        max_nonce_retries = 25

        for attempt in range(max_scan if not self._user_index_found else 1):
            idx = self._user_index + (attempt if not self._user_index_found else 0)

            for nonce_try in range(max_nonce_retries):
                result = await self._broadcast_once(msg, idx)
                err = self._parse_broadcast_error(result)

                if err is None:
                    if not self._user_index_found:
                        self._user_index = idx
                        self._user_index_found = True
                        logger.info("Dango user_index 확정: %d", idx)
                    return result

                if "nonce is too far ahead" in err or "nonce is already seen" in err:
                    chain_nonce = self._extract_chain_nonce(err)
                    if chain_nonce is not None and nonce_try < max_nonce_retries - 1:
                        async with self._nonce_lock:
                            # already seen이면 그 nonce 자체가 사용됨 → 다음 시도는 +1
                            # too far ahead면 chain_nonce가 max seen → 다음은 +1
                            self._nonce = chain_nonce
                        logger.warning("Dango 논스 자동 보정: %d → 다음=%d", chain_nonce, chain_nonce + 1)
                        continue

                break  # 논스 이외 에러 또는 재시도 소진

            err = self._parse_broadcast_error(result)
            if err is None:
                return result

            if "isn't associated with user" in err and not self._user_index_found:
                logger.warning("Dango user_index %d 불일치, 다음 시도...", idx)
                continue

            logger.warning("Dango tx rejected (index=%d): %s", idx, err)
            return result

        raise RuntimeError(
            f"Dango user_index를 찾을 수 없습니다 (0~{max_scan-1} 모두 실패). "
            f".env에 DANGO_USER_INDEX=<올바른값> 을 설정하세요."
        )

    # ──────────────────────────────────────────────
    # REST 조회 헬퍼
    # ──────────────────────────────────────────────

    _ACCOUNT_FACTORY = "0x18d28bafcdf9d4574f920ea004dea2d13ec16f6b"

    async def _query_app(self, msg: dict, contract: Optional[str] = None) -> Any:
        # GrugQueryInput은 GraphQL object literal 방식으로만 동작함 (JSON string 변수 불가)
        target = contract or self._contract
        msg_literal = _to_gql_literal(msg)
        query = (
            "{queryApp(request:{wasm_smart:{contract:"
            + json.dumps(target)
            + ",msg:"
            + msg_literal
            + "}})}"
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

    async def _load_key_info(self) -> None:
        """ACCOUNT_FACTORY에서 user_index, key_hash, key_type 로드."""
        env_val = os.environ.get("DANGO_USER_INDEX", "")
        if not (env_val and env_val.isdigit() and int(env_val) > 0):
            logger.warning("DANGO_USER_INDEX 미설정 — .env에 설정 필요")
            return

        user_idx = int(env_val)
        self._user_index = user_idx
        logger.info("Dango user_index: %d", user_idx)

        try:
            result = await self._query_app(
                {"user": {"index": user_idx}},
                contract=self._ACCOUNT_FACTORY,
            )
            keys: dict = (result or {}).get("keys", {})
            if not keys:
                raise ValueError("factory 응답에 keys 없음")

            key_hash = next(iter(keys.keys()))
            key_info = next(iter(keys.values()))
            key_type = next(iter(key_info.keys()))  # "ethereum" | "secp256k1"

            self._key_hash = key_hash
            self._key_type = key_type
            self._user_index_found = True
            logger.info("Dango 키 로드: hash=%s type=%s", key_hash[:16] + "...", key_type)
        except Exception as e:
            logger.warning("factory 키 조회 실패 (%s) — 로컬 계산으로 폴백", e)
            # 로컬 폴백: private key에서 직접 계산
            self._key_hash = _derive_key_hash_ethereum(self._pk)
            self._key_type = "ethereum"
            self._user_index_found = True
            logger.info("Dango 키 로컬 계산: hash=%s type=%s",
                        self._key_hash[:16] + "...", self._key_type)

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

    @staticmethod
    def make_client_order_id() -> str:
        """Dango contract는 client_order_id를 u64로 파싱 — 숫자 문자열 (ns timestamp + 랜덤 3자리)."""
        import time as _time
        import random as _random
        return str(_time.time_ns() // 1000 * 1000 + _random.randint(0, 999))

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
        """Maker 지정가 주문 전송. client_order_id 반환 (u64 숫자 문자열)."""
        cid = client_order_id or self.make_client_order_id()
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
        result = await self._broadcast(msg)
        err = self._parse_broadcast_error(result)
        if err:
            raise RuntimeError(f"Dango limit order failed (check_tx): {err}")
        tx_hash = (result or {}).get("tx_hash", "?")

        # check_tx 통과 후에도 deliver_tx에서 실패할 수 있음 — indexer로 확정
        deliver_err = await self._verify_tx_committed(tx_hash)
        if deliver_err:
            raise RuntimeError(f"Dango limit order failed (deliver_tx): {deliver_err}")

        logger.info("Dango limit order placed: %s %s %s@%.4f cid=%s tx=%s",
                    pair_id, side, size, price, cid, tx_hash[:16])
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
                    ping_interval=10,
                    ping_timeout=10,
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

                    # Dango 서버 30s "registered timeout" 회피: 15s마다 ping 송신
                    async def _keepalive():
                        try:
                            while True:
                                await asyncio.sleep(_GQL_KEEPALIVE_INTERVAL)
                                await ws.send(json.dumps({"type": _GQL_PING}))
                        except (asyncio.CancelledError, ConnectionClosed):
                            return

                    keepalive_task = asyncio.create_task(_keepalive())

                    try:
                        async for raw in ws:
                            msg = json.loads(raw)
                            msg_type = msg.get("type")
                            if msg_type == _GQL_NEXT:
                                payload = msg.get("payload", {})
                                events_data = payload.get("data", {}).get("events")
                                if events_data and events_data.get("type") == "order_filled":
                                    self._on_fill_event(events_data)
                            elif msg_type == _GQL_PING:
                                # 서버 ping → pong 응답 (graphql-transport-ws spec)
                                await ws.send(json.dumps({"type": _GQL_PONG}))
                            elif msg_type == _GQL_PONG:
                                pass  # 우리가 보낸 ping에 대한 응답 — 무시
                            elif msg_type == _GQL_ERROR:
                                logger.error("Dango WS 구독 에러: %s", msg.get("payload"))
                            elif msg_type == _GQL_COMPLETE:
                                logger.warning("Dango WS 구독 종료됨")
                                break
                    finally:
                        keepalive_task.cancel()

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
        """WebSocket 이벤트 구독 시작 + key_hash/type/user_index 확정"""
        self._running = True
        await self._load_key_info()
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
