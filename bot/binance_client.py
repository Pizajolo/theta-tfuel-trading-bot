"""Minimal async Binance Spot REST client (httpx) with HMAC-SHA256 signing and rate-limit handling.

* Request weight is tracked from the ``X-MBX-USED-WEIGHT-1M`` header (limits are per IP, so the
  tracker is shared by every client in the process). Above 80% of the limit we wait for the
  next minute.
* 429 / 418 responses: honour ``Retry-After``; idempotent GETs are retried once the wait is
  short, anything else raises :class:`RateLimitError`.
* 5xx or a transport error on an order placement means "execution status unknown" and raises
  :class:`UnknownOrderStatus` - callers must query the order by its client order id.
* ``-1021`` (timestamp outside recvWindow) triggers a server-time resync and one retry.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import random
import time
from typing import Any
from urllib.parse import urlencode

import httpx

log = logging.getLogger("bot.binance")


class BinanceAPIError(Exception):
    def __init__(self, status: int, code: int | None, msg: str) -> None:
        super().__init__(f"HTTP {status} code={code} {msg}")
        self.status = status
        self.code = code
        self.msg = msg


class RateLimitError(BinanceAPIError):
    def __init__(self, status: int, retry_after: float, msg: str = "rate limited") -> None:
        super().__init__(status, None, msg)
        self.retry_after = retry_after


class UnknownOrderStatus(Exception):
    """The order request may or may not have reached the matching engine."""


class WeightTracker:
    def __init__(self, limit_1m: int = 6000) -> None:
        self.limit_1m = limit_1m
        self.used_1m = 0
        self.updated_at = 0.0
        self.blocked_until = 0.0  # monotonic seconds

    def update(self, headers: httpx.Headers) -> None:
        v = headers.get("x-mbx-used-weight-1m")
        if v is not None:
            try:
                self.used_1m = int(v)
                self.updated_at = time.time()
            except ValueError:
                pass

    def wait_seconds(self) -> float:
        now_mono = time.monotonic()
        if self.blocked_until > now_mono:
            return self.blocked_until - now_mono
        # Header counts reset at each full minute.
        if self.used_1m >= 0.8 * self.limit_1m and int(self.updated_at // 60) == int(time.time() // 60):
            return 60.0 - (time.time() % 60) + 0.5
        return 0.0


WEIGHTS = WeightTracker()


class BinanceClient:
    def __init__(
        self,
        base_url: str,
        api_key: str = "",
        api_secret: str = "",
        timeout: float = 10.0,
        recv_window: int = 5000,
        transport: httpx.AsyncBaseTransport | None = None,
        weights: WeightTracker | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._key = api_key
        self._secret = api_secret.encode() if api_secret else b""
        self.recv_window = recv_window
        self.time_offset_ms = 0
        self.weights = weights or WEIGHTS
        headers = {"User-Agent": "theta-tfuel-ratio-bot/1.0"}
        if api_key:
            headers["X-MBX-APIKEY"] = api_key
        self._http = httpx.AsyncClient(base_url=self.base_url, timeout=timeout, headers=headers, transport=transport)

    def __repr__(self) -> str:  # never leak keys
        return f"BinanceClient({self.base_url}, key={'set' if self._key else 'none'})"

    @property
    def is_testnet(self) -> bool:
        return "testnet" in self.base_url

    async def close(self) -> None:
        await self._http.aclose()

    # ---- core --------------------------------------------------------------------------
    def _sign(self, params: dict[str, Any]) -> str:
        params = {k: v for k, v in params.items() if v is not None}
        params["recvWindow"] = self.recv_window
        params["timestamp"] = int(time.time() * 1000) + self.time_offset_ms
        query = urlencode(params)
        sig = hmac.new(self._secret, query.encode(), hashlib.sha256).hexdigest()
        return f"{query}&signature={sig}"

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        signed: bool = False,
        retries: int = 3,
        is_order: bool = False,
    ) -> Any:
        params = dict(params or {})
        attempt = 0
        resynced = False
        while True:
            attempt += 1
            wait = self.weights.wait_seconds()
            if wait > 0:
                if wait > 120 and not is_order:
                    raise RateLimitError(418, wait, "IP temporarily banned")
                log.warning("rate-limit backoff %.1fs before %s %s", wait, method, path)
                await asyncio.sleep(wait)
            if signed:
                if not self._secret:
                    raise BinanceAPIError(0, None, "signed endpoint requires API credentials")
                url = f"{path}?{self._sign(params)}"
                req_params = None
            else:
                url = path
                req_params = {k: v for k, v in params.items() if v is not None}
            try:
                resp = await self._http.request(method, url, params=req_params)
            except httpx.TransportError as exc:
                if is_order:
                    raise UnknownOrderStatus(f"{method} {path}: {exc!r}") from exc
                if attempt <= retries:
                    await asyncio.sleep(min(2 ** attempt, 10) + random.random())
                    continue
                raise BinanceAPIError(0, None, f"transport error: {exc!r}") from exc

            self.weights.update(resp.headers)
            if resp.status_code in (429, 418):
                retry_after = float(resp.headers.get("retry-after", "60") or 60)
                self.weights.blocked_until = time.monotonic() + retry_after
                log.error("Binance returned %s, retry after %ss", resp.status_code, retry_after)
                if not is_order and method == "GET" and retry_after <= 60 and attempt <= retries:
                    continue
                raise RateLimitError(resp.status_code, retry_after)
            if resp.status_code >= 500:
                if is_order:
                    raise UnknownOrderStatus(f"{method} {path}: HTTP {resp.status_code}")
                if attempt <= retries:
                    await asyncio.sleep(min(2 ** attempt, 10) + random.random())
                    continue
            try:
                data = resp.json()
            except ValueError:
                data = None
            if resp.status_code >= 400:
                code = data.get("code") if isinstance(data, dict) else None
                msg = data.get("msg", resp.text[:200]) if isinstance(data, dict) else resp.text[:200]
                if code == -1021 and signed and not resynced:
                    resynced = True
                    await self.sync_time()
                    continue
                if code == -1007 and is_order:  # backend timeout: execution status unknown
                    raise UnknownOrderStatus(msg)
                raise BinanceAPIError(resp.status_code, code, msg)
            return data

    # ---- public ---------------------------------------------------------------------
    async def ping(self) -> Any:
        return await self._request("GET", "/api/v3/ping")

    async def server_time(self) -> int:
        return int((await self._request("GET", "/api/v3/time"))["serverTime"])

    async def sync_time(self) -> int:
        t0 = time.time() * 1000
        st = await self.server_time()
        t1 = time.time() * 1000
        self.time_offset_ms = int(st - (t0 + t1) / 2)
        return self.time_offset_ms

    async def exchange_info(self, symbols: list[str] | tuple[str, ...]) -> dict:
        import json as _json

        return await self._request(
            "GET", "/api/v3/exchangeInfo", {"symbols": _json.dumps(list(symbols), separators=(",", ":"))}
        )

    async def klines(
        self, symbol: str, interval: str, start_ms: int | None = None, end_ms: int | None = None, limit: int = 1000
    ) -> list[list]:
        return await self._request(
            "GET",
            "/api/v3/klines",
            {"symbol": symbol, "interval": interval, "startTime": start_ms, "endTime": end_ms, "limit": limit},
        )

    async def klines_range(self, symbol: str, interval: str, start_ms: int, end_ms: int) -> list[list]:
        """All klines with open time in [start_ms, end_ms], paginated."""
        out: list[list] = []
        cursor = start_ms
        while cursor <= end_ms:
            batch = await self.klines(symbol, interval, cursor, end_ms, 1000)
            if not batch:
                break
            out.extend(k for k in batch if k[0] <= end_ms)
            last_open = int(batch[-1][0])
            if last_open < cursor or len(batch) < 1000:
                break
            cursor = last_open + 1
        dedup: dict[int, list] = {int(k[0]): k for k in out}
        return [dedup[t] for t in sorted(dedup)]

    async def book_ticker(self, symbol: str) -> dict:
        return await self._request("GET", "/api/v3/ticker/bookTicker", {"symbol": symbol})

    async def ticker_price(self, symbol: str) -> float:
        return float((await self._request("GET", "/api/v3/ticker/price", {"symbol": symbol}))["price"])

    # ---- signed -------------------------------------------------------------------------
    async def account(self) -> dict:
        return await self._request("GET", "/api/v3/account", {"omitZeroBalances": "true"}, signed=True)

    async def api_restrictions(self) -> dict:
        """API key permissions (mainnet only; SAPI is not available on the Spot testnet)."""
        return await self._request("GET", "/sapi/v1/account/apiRestrictions", signed=True)

    async def new_order(self, **params: Any) -> dict:
        return await self._request("POST", "/api/v3/order", params, signed=True, is_order=True)

    async def get_order(self, symbol: str, orig_client_order_id: str) -> dict | None:
        """Query by client order id; returns None when Binance does not know the order (-2013)."""
        try:
            return await self._request(
                "GET", "/api/v3/order", {"symbol": symbol, "origClientOrderId": orig_client_order_id}, signed=True
            )
        except BinanceAPIError as exc:
            if exc.code == -2013:
                return None
            raise

    async def open_orders(self, symbol: str) -> list[dict]:
        return await self._request("GET", "/api/v3/openOrders", {"symbol": symbol}, signed=True)

    async def cancel_order(self, symbol: str, orig_client_order_id: str) -> dict:
        return await self._request(
            "DELETE", "/api/v3/order", {"symbol": symbol, "origClientOrderId": orig_client_order_id}, signed=True
        )

    async def cancel_open_orders(self, symbol: str) -> list[dict]:
        try:
            return await self._request("DELETE", "/api/v3/openOrders", {"symbol": symbol}, signed=True)
        except BinanceAPIError as exc:
            if exc.code == -2011:  # nothing to cancel
                return []
            raise

    async def my_trades(self, symbol: str, order_id: int) -> list[dict]:
        return await self._request("GET", "/api/v3/myTrades", {"symbol": symbol, "orderId": order_id}, signed=True)
