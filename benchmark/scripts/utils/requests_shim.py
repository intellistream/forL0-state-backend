#!/usr/bin/env python3
"""
Drop-in replacement for the ``requests`` library, backed by urllib3.

The ``requests`` package has a compatibility issue with the Flink REST server
(RemoteDisconnected errors), while urllib3 works correctly.  This module exposes
the same ``get / post / patch / delete`` API so that existing scripts can simply
replace ``import requests`` with ``from utils import requests_shim as requests``.
"""

import json as _json
import urllib.parse as _urlparse
import urllib3  # type: ignore[import-untyped]
from typing import Any, Dict, Optional

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_pool = urllib3.PoolManager(
    timeout=urllib3.Timeout(connect=10, read=60),
    num_pools=4,
)


# ── Response wrapper ──────────────────────────────────────────────────────────

class _Response:
    """Minimal requests-compatible response wrapper."""

    def __init__(self, urllib3_resp):
        self.status_code: int = urllib3_resp.status
        self._data: bytes = urllib3_resp.data or b''
        self.text: str = self._data.decode('utf-8', errors='replace')

    def json(self) -> Any:
        return _json.loads(self._data.decode('utf-8'))

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f'HTTP {self.status_code}')


# ── Helpers ───────────────────────────────────────────────────────────────────

def _build_url(url: str, params: Optional[dict] = None) -> str:
    if params:
        sep = '&' if '?' in url else '?'
        qs = _urlparse.urlencode(params)
        return f'{url}{sep}{qs}'
    return url


def _timeout(timeout) -> urllib3.Timeout:
    if timeout is None:
        return urllib3.Timeout(connect=10, read=60)
    if isinstance(timeout, (int, float)):
        return urllib3.Timeout(connect=min(timeout, 10), read=timeout)
    return urllib3.Timeout(connect=10, read=60)


# ── Public API ────────────────────────────────────────────────────────────────

def get(url: str, *, params: Optional[dict] = None, timeout: Any = None,
        headers: Optional[dict] = None, **_kw) -> _Response:
    return _Response(_pool.request(
        'GET', _build_url(url, params), timeout=_timeout(timeout), headers=headers))


def post(url: str, *, json: Any = None, data: Any = None,
         params: Optional[dict] = None, timeout: Any = None,
         files: Optional[dict] = None, headers: Optional[dict] = None, **_kw) -> _Response:
    kw: Dict[str, Any] = {'timeout': _timeout(timeout)}
    if headers:
        kw['headers'] = headers
    if json is not None:
        kw['body'] = _json.dumps(json).encode('utf-8')
        kw.setdefault('headers', {})['Content-Type'] = 'application/json'
    elif data is not None:
        kw['body'] = data
    if files is not None:
        # Convert requests-style files dict to urllib3 multipart fields
        fields = {}
        for key, val in files.items():
            if isinstance(val, tuple):
                fname, fobj, ctype = val[0], val[1], val[2] if len(val) > 2 else 'application/octet-stream'
                fields[key] = (fname, fobj.read() if hasattr(fobj, 'read') else fobj, ctype)
            else:
                fields[key] = val
        kw['fields'] = fields
        kw.pop('body', None)
    return _Response(_pool.request('POST', _build_url(url, params), **kw))


def patch(url: str, *, params: Optional[dict] = None, timeout: Any = None,
          json: Any = None, headers: Optional[dict] = None, **_kw) -> _Response:
    kw: Dict[str, Any] = {'timeout': _timeout(timeout)}
    if headers:
        kw['headers'] = headers
    if json is not None:
        kw['body'] = _json.dumps(json).encode('utf-8')
        kw.setdefault('headers', {})['Content-Type'] = 'application/json'
    return _Response(_pool.request('PATCH', _build_url(url, params), **kw))


def delete(url: str, *, params: Optional[dict] = None, timeout: Any = None,
           headers: Optional[dict] = None, **_kw) -> _Response:
    return _Response(_pool.request(
        'DELETE', _build_url(url, params), timeout=_timeout(timeout), headers=headers))
