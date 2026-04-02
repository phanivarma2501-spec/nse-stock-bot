"""
Turso HTTP API client — drop-in async replacement for aiosqlite.
Uses Turso's HTTP pipeline API. Falls back to aiosqlite when TURSO_DATABASE_URL is not set.
"""

import os
import httpx
import aiosqlite
from contextlib import asynccontextmanager
from loguru import logger

def _get_turso_config():
    """Read Turso config from env vars at call time (not import time)."""
    url = os.environ.get("TURSO_DATABASE_URL", "")
    token = os.environ.get("TURSO_AUTH_TOKEN", "")
    return url, token


def _turso_http_url(url):
    """Convert libsql:// URL to https:// for HTTP API."""
    if url.startswith("libsql://"):
        url = url.replace("libsql://", "https://")
    return url.rstrip("/")


class TursoConnection:
    """Async connection to Turso via HTTP pipeline API."""

    def __init__(self, url, token):
        self._base = _turso_http_url(url)
        self._client = httpx.AsyncClient(
            timeout=30,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
        self._pending = []
        self.row_factory = None

    def execute(self, sql: str, params=None):
        """Execute a SQL statement. Returns an awaitable that also works as async context manager."""
        return _TursoExecutable(self, sql, params)

    async def _do_execute(self, sql: str, params=None):
        """Actually execute the SQL."""
        stmt = {"sql": sql}
        if params:
            stmt["args"] = [_convert_param(p) for p in params]
        body = {"requests": [
            {"type": "execute", "stmt": stmt},
            {"type": "close"},
        ]}
        resp = await self._client.post(f"{self._base}/v3/pipeline", json=body)
        if resp.status_code != 200:
            raise Exception(f"Turso HTTP {resp.status_code}: {resp.text[:200]}")
        data = resp.json()
        results = data.get("results", [])
        if results and results[0].get("type") == "error":
            err = results[0].get("error", {})
            raise Exception(f"Turso SQL error: {err.get('message', str(err))}")
        if results and results[0].get("type") == "ok":
            response = results[0].get("response", {})
            result = response.get("result", {})
            return TursoCursor(result, self.row_factory)
        return TursoCursor({}, self.row_factory)

    async def commit(self):
        """No-op — Turso auto-commits."""
        pass

    async def close(self):
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()


class _TursoExecutable:
    """Supports both `cursor = await db.execute(...)` and `async with db.execute(...) as cursor`."""
    def __init__(self, conn, sql, params):
        self._conn = conn
        self._sql = sql
        self._params = params
        self._cursor = None

    def __await__(self):
        return self._conn._do_execute(self._sql, self._params).__await__()

    async def __aenter__(self):
        self._cursor = await self._conn._do_execute(self._sql, self._params)
        return self._cursor

    async def __aexit__(self, *args):
        pass


class TursoCursor:
    """Mimics aiosqlite cursor for compatibility."""

    def __init__(self, result, row_factory=None):
        self._cols = [c.get("name") for c in result.get("cols", [])]
        self._rows = []
        for row in result.get("rows", []):
            values = [_extract_value(cell) for cell in row]
            if row_factory:
                # Return dict-like rows (compatible with aiosqlite.Row and dict())
                self._rows.append(_DictRow(self._cols, values))
            else:
                self._rows.append(tuple(values))

    async def fetchall(self):
        return self._rows

    async def fetchone(self):
        return self._rows[0] if self._rows else None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class _DictRow(dict):
    """Row that supports both dict key access and index access."""
    def __init__(self, cols, values):
        super().__init__(zip(cols, values))
        self._values = values
    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return super().__getitem__(key)


def _convert_param(value):
    """Convert Python value to Turso API format."""
    if value is None:
        return {"type": "null", "value": None}
    elif isinstance(value, int):
        return {"type": "integer", "value": str(value)}
    elif isinstance(value, float):
        return {"type": "float", "value": value}
    elif isinstance(value, str):
        return {"type": "text", "value": value}
    elif isinstance(value, bytes):
        import base64
        return {"type": "blob", "base64": base64.b64encode(value).decode()}
    else:
        return {"type": "text", "value": str(value)}


def _extract_value(cell):
    """Extract Python value from Turso API response cell."""
    if cell is None:
        return None
    t = cell.get("type", "")
    v = cell.get("value")
    if t == "null" or v is None:
        return None
    elif t == "integer":
        return int(v)
    elif t == "float":
        return float(v)
    elif t == "text":
        return str(v)
    elif t == "blob":
        import base64
        return base64.b64decode(v)
    return v


class _AiosqliteWrapper:
    """Wraps aiosqlite connection so row_factory=True works (maps to aiosqlite.Row)."""
    def __init__(self, db):
        self._db = db
    @property
    def row_factory(self):
        return self._db.row_factory
    @row_factory.setter
    def row_factory(self, value):
        if value is True:
            self._db.row_factory = aiosqlite.Row
        else:
            self._db.row_factory = value
    def execute(self, sql, params=None):
        if params:
            return self._db.execute(sql, params)
        return self._db.execute(sql)
    async def commit(self):
        await self._db.commit()
    async def close(self):
        pass  # managed by aiosqlite context manager


@asynccontextmanager
async def connect(local_path=None):
    """
    Smart connection: uses Turso when configured, aiosqlite otherwise.
    Provides the same async context manager interface.
    """
    turso_url, turso_token = _get_turso_config()
    if turso_url and turso_token:
        conn = TursoConnection(turso_url, turso_token)
        try:
            yield conn
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(local_path) as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA busy_timeout=5000")
            yield _AiosqliteWrapper(db)
