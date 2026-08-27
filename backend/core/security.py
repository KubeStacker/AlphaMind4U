"""
静态 Bearer Token 认证模块。
"""
import secrets

from fastapi import Request

from core.config import settings


def is_valid_static_token(token: str) -> bool:
    static_token = settings.jarvis_static_auth_token.strip()
    return bool(static_token) and secrets.compare_digest(token.strip(), static_token)


def is_valid_static_auth_request(request: Request) -> bool:
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return False
    return is_valid_static_token(auth_header[7:])
