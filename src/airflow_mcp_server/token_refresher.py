from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
import time

import aiohttp

logger = logging.getLogger(__name__)

# Refresh once half the token's remaining lifetime has elapsed, never more often than
# every 60s, and fall back to hourly refreshes if a token's `exp` claim can't be read.
REFRESH_MARGIN = 0.5
MIN_REFRESH_INTERVAL = 60
FALLBACK_REFRESH_INTERVAL = 3600


def decode_jwt_exp(token: str) -> float | None:
    """Return a JWT's `exp` claim (unix timestamp) without verifying its signature."""
    try:
        payload_b64 = token.split(".")[1]
        padding = "=" * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64 + padding))
        return float(payload["exp"])
    except (IndexError, ValueError, KeyError, TypeError):
        return None


async def fetch_token(session: aiohttp.ClientSession, username: str, password: str) -> str:
    """Log in to Airflow's `/auth/token` endpoint and return the issued JWT."""
    async with session.post("/auth/token", json={"username": username, "password": password}) as response:
        response.raise_for_status()
        payload = await response.json()

    for key in ("access_token", "token", "jwt"):
        if key in payload:
            return str(payload[key])
    raise ValueError(f"Unexpected response from /auth/token: no access token found in keys {list(payload)}")


class TokenRefresher:
    """Keeps an aiohttp.ClientSession's Authorization header populated with a live Airflow JWT.

    Fetches an initial token synchronously on `start()`, then re-authenticates in the
    background before each token expires - so the session's Bearer header stays valid
    for the life of a long-running process without ever needing a restart.
    """

    def __init__(self, session: aiohttp.ClientSession, username: str, password: str) -> None:
        self._session = session
        self._username = username
        self._password = password
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        exp = await self._refresh_once()
        self._task = asyncio.create_task(self._loop(exp))

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _loop(self, exp: float | None) -> None:
        while True:
            await asyncio.sleep(self._sleep_duration(exp))
            try:
                exp = await self._refresh_once()
            except Exception:
                logger.exception("Failed to refresh Airflow JWT; retrying in %ss", MIN_REFRESH_INTERVAL)
                exp = time.time() + MIN_REFRESH_INTERVAL

    async def _refresh_once(self) -> float | None:
        token = await fetch_token(self._session, self._username, self._password)
        self._session.headers["Authorization"] = f"Bearer {token}"
        exp = decode_jwt_exp(token)
        if exp is not None:
            logger.info("Refreshed Airflow JWT, expires %s UTC", time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(exp)))
        else:
            logger.info("Refreshed Airflow JWT (no readable expiry claim)")
        return exp

    @staticmethod
    def _sleep_duration(exp: float | None) -> float:
        if exp is None:
            return FALLBACK_REFRESH_INTERVAL
        remaining = exp - time.time()
        return max(remaining * REFRESH_MARGIN, MIN_REFRESH_INTERVAL)
