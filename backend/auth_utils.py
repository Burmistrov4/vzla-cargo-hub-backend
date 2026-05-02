from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from datetime import datetime, timezone

PASSWORD_HASH_ALGORITHM = "pbkdf2_sha256"
PASSWORD_HASH_ITERATIONS = 260_000
SESSION_TOKEN_BYTES = 32
DEFAULT_SESSION_HOURS = 12


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_datetime(value) -> datetime | None:
    if not value:
        return None

    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    try:
        text = str(value).strip().replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def normalize_username(username: str | None) -> str:
    return (username or "").strip().lower()


def hash_password(password: str) -> str:
    if not password:
        raise ValueError("La contraseña no puede estar vacía")

    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        PASSWORD_HASH_ITERATIONS,
    ).hex()

    return f"{PASSWORD_HASH_ALGORITHM}${PASSWORD_HASH_ITERATIONS}${salt}${digest}"


def verify_password(password: str, stored_hash: str | None) -> bool:
    if not password or not stored_hash:
        return False

    try:
        algorithm, iterations_text, salt, expected_digest = stored_hash.split("$", 3)
        if algorithm != PASSWORD_HASH_ALGORITHM:
            return False

        iterations = int(iterations_text)
        actual_digest = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            salt.encode("utf-8"),
            iterations,
        ).hex()

        return hmac.compare_digest(actual_digest, expected_digest)
    except Exception:
        return False


def generate_session_token() -> str:
    return secrets.token_urlsafe(SESSION_TOKEN_BYTES)


def get_session_hours() -> int:
    raw_value = os.getenv("APP_AUTH_SESSION_HOURS")
    if not raw_value:
        return DEFAULT_SESSION_HOURS

    try:
        hours = int(raw_value)
        return max(1, min(hours, 24 * 30))
    except ValueError:
        return DEFAULT_SESSION_HOURS


def env_flag_enabled(name: str) -> bool:
    value = (os.getenv(name) or "").strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def seed_token_matches(header_value: str | None) -> bool:
    expected = os.getenv("APP_AUTH_SEED_TOKEN")
    if not expected:
        return True

    return bool(header_value) and hmac.compare_digest(header_value, expected)
