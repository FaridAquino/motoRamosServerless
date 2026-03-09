"""
MotoRamos — Utilidades JWT para WebSocket.
Solo verifica tokens (no genera) — los tokens se generan en los servicios REST.
"""

import json
import hmac
import hashlib
import base64
import time
import os

JWT_SECRET = os.environ.get('JWT_SECRET', 'motoRamos-tarma-jwt-secret-utec-2026')


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode()


def _b64url_decode(s: str) -> bytes:
    s += '=' * (4 - len(s) % 4)
    return base64.urlsafe_b64decode(s)


def verify_jwt(token: str) -> dict | None:
    """Verifica un JWT HS256. Retorna el payload o None si es inválido/expirado."""
    try:
        parts = token.split('.')
        if len(parts) != 3:
            return None
        signing_input = f'{parts[0]}.{parts[1]}'.encode()
        expected = hmac.new(JWT_SECRET.encode(), signing_input, hashlib.sha256).digest()
        actual = _b64url_decode(parts[2])
        if not hmac.compare_digest(expected, actual):
            return None
        payload = json.loads(_b64url_decode(parts[1]))
        if payload.get('exp', 0) < time.time():
            return None
        return payload
    except Exception:
        return None
