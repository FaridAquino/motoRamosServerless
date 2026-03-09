"""
MotoRamos — Utilidades compartidas de Autenticación y Respuesta HTTP.
Proyecto de ingeniería UTEC para la ciudad de Tarma, Junín (3 048 m s.n.m.).
Python 3.12 | JWT HS256 | PBKDF2-SHA256 (600 000 iteraciones)
"""

import json
import hmac
import hashlib
import base64
import time
import os
import functools

# ─── Configuración ───────────────────────────────────────────────────────────
JWT_SECRET = os.environ.get('JWT_SECRET', 'motoRamos-tarma-jwt-secret-utec-2026')
JWT_EXPIRY_HOURS = 72  # 3 días — ventana amplia para conectividad intermitente en la sierra

PBKDF2_ITERATIONS = 600_000

CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
    'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
}


# ═══════════════════════════════════════════════════════════════════════════════
# JWT (HS256 implementado sin dependencias externas)
# ═══════════════════════════════════════════════════════════════════════════════

def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode()


def _b64url_decode(s: str) -> bytes:
    s += '=' * (4 - len(s) % 4)
    return base64.urlsafe_b64decode(s)


def generate_jwt(payload: dict, expiry_hours: int | None = None) -> str:
    exp_h = expiry_hours if expiry_hours is not None else JWT_EXPIRY_HOURS
    header = {'alg': 'HS256', 'typ': 'JWT'}
    now = int(time.time())
    full_payload = {**payload, 'iat': now, 'exp': now + exp_h * 3600}
    segments = [
        _b64url_encode(json.dumps(header, separators=(',', ':')).encode()),
        _b64url_encode(json.dumps(full_payload, separators=(',', ':')).encode()),
    ]
    signing_input = f'{segments[0]}.{segments[1]}'.encode()
    sig = hmac.new(JWT_SECRET.encode(), signing_input, hashlib.sha256).digest()
    segments.append(_b64url_encode(sig))
    return '.'.join(segments)


def verify_jwt(token: str) -> dict | None:
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


# ═══════════════════════════════════════════════════════════════════════════════
# Hashing de contraseñas (PBKDF2-SHA256, 600 000 iteraciones)
# ═══════════════════════════════════════════════════════════════════════════════

def hash_password(password: str) -> str:
    salt = os.urandom(16)
    h = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, PBKDF2_ITERATIONS)
    return f'{salt.hex()}:{h.hex()}'


def verify_password(password: str, stored: str) -> bool:
    try:
        salt_hex, hash_hex = stored.split(':')
        expected = bytes.fromhex(hash_hex)
        actual = hashlib.pbkdf2_hmac(
            'sha256', password.encode(), bytes.fromhex(salt_hex), PBKDF2_ITERATIONS
        )
        return hmac.compare_digest(expected, actual)
    except (ValueError, AttributeError):
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers de respuesta HTTP
# ═══════════════════════════════════════════════════════════════════════════════

def _json_serial(obj):
    from decimal import Decimal as D
    if isinstance(obj, D):
        return float(obj)
    if isinstance(obj, set):
        return list(obj)
    raise TypeError(f'Tipo no serializable: {type(obj)}')


def response(status_code: int, body: dict) -> dict:
    return {
        'statusCode': status_code,
        'headers': CORS_HEADERS,
        'body': json.dumps(body, default=_json_serial, ensure_ascii=False),
    }


def success(body: dict, status_code: int = 200) -> dict:
    return response(status_code, body)


def error(message: str, status_code: int = 400) -> dict:
    return response(status_code, {'error': message})


# ═══════════════════════════════════════════════════════════════════════════════
# Decorator de autenticación JWT
# ═══════════════════════════════════════════════════════════════════════════════

def require_auth(fn):
    @functools.wraps(fn)
    def wrapper(event, context):
        headers = event.get('headers') or {}
        auth = headers.get('Authorization') or headers.get('authorization') or ''
        if not auth.startswith('Bearer '):
            return error('Token de autorización requerido', 401)
        claims = verify_jwt(auth[7:])
        if claims is None:
            return error('Token inválido o expirado', 401)
        event['authClaims'] = claims
        return fn(event, context)
    return wrapper


# ═══════════════════════════════════════════════════════════════════════════════
# Utilidades de parsing
# ═══════════════════════════════════════════════════════════════════════════════

def extract_body(event) -> dict | None:
    raw = event.get('body')
    if raw is None:
        return None
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return None
    return raw if isinstance(raw, dict) else None
