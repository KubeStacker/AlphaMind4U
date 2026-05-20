"""
静态 Bearer Token 认证模块。
"""
import secrets

from fastapi import HTTPException, Request, status

from core.config import settings


def _get_user_id_by_username(username: str) -> int:
    from db.connection import get_db_connection

    with get_db_connection() as con:
        user = con.execute(
            "SELECT id FROM users WHERE username = ?", (username,)
        ).fetchone()
        if not user:
            raise HTTPException(status_code=401, detail="用户不存在")
        return user[0]


def get_static_auth_username() -> str:
    username = settings.jarvis_static_auth_username.strip()
    if not username:
        raise HTTPException(status_code=401, detail="静态令牌未配置用户")
    return username


def is_valid_static_token(token: str) -> bool:
    static_token = settings.jarvis_static_auth_token.strip()
    return bool(static_token) and secrets.compare_digest(token.strip(), static_token)


def is_valid_static_auth_request(request: Request) -> bool:
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return False
    return is_valid_static_token(auth_header[7:])


async def get_current_user_id(request: Request) -> int:
    """从 Authorization header 中校验静态令牌，返回配置用户的 user_id。"""
    if not is_valid_static_auth_request(request):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="静态令牌无效或缺失",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return _get_user_id_by_username(get_static_auth_username())
