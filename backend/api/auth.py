# /backend/api/auth.py

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from typing import Annotated
import datetime
from passlib.context import CryptContext
from db.connection import get_db_connection

router = APIRouter(prefix="/auth", tags=["Authentication"])
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

@router.post("/token")
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()]
):
    """
    用户登录接口，返回 token 和角色信息。
    """
    with get_db_connection() as con:
        user = con.execute(
            "SELECT hashed_password, role FROM users WHERE username = ?", 
            (form_data.username,)
        ).fetchone()
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="用户不存在",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        hashed_password, role = user
        
        if not pwd_context.verify(form_data.password, hashed_password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="密码错误",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # 简化版 Token 逻辑
        access_token_expires = datetime.timedelta(minutes=60)
        expire = datetime.datetime.utcnow() + access_token_expires
        dummy_jwt = f"token.{form_data.username}.{role}.{int(expire.timestamp())}"
        
        return {
            "access_token": dummy_jwt, 
            "token_type": "bearer",
            "username": form_data.username,
            "role": role
        }

