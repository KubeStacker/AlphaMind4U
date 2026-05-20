# /backend/api/auth.py

from fastapi import APIRouter, HTTPException, Request, status

from passlib.context import CryptContext
from db.connection import get_db_connection
from core.config import settings

router = APIRouter(prefix="/auth", tags=["Authentication"])
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


@router.post("/token")
async def login_for_access_token(
    request: Request,
):
    """用户登录接口，返回静态 Bearer token 和角色信息。"""
    static_username = settings.jarvis_static_auth_username.strip()
    static_token = settings.jarvis_static_auth_token.strip()
    if not static_username or not static_token:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="静态令牌未配置",
        )

    form = await request.form()
    username = str(form.get("username") or "").strip()
    password = str(form.get("password") or "")

    if username != static_username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="用户不存在",
            headers={"WWW-Authenticate": "Bearer"},
        )

    with get_db_connection() as con:
        user = con.execute(
            "SELECT hashed_password, role FROM users WHERE username = ?",
            (static_username,)
        ).fetchone()

        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="用户不存在",
                headers={"WWW-Authenticate": "Bearer"},
            )

        hashed_password, role = user

        if password and not pwd_context.verify(password, hashed_password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="密码错误",
                headers={"WWW-Authenticate": "Bearer"},
            )

        return {
            "access_token": static_token,
            "token_type": "bearer",
            "username": static_username,
            "role": role,
        }
