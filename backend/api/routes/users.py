# /backend/api/routes/users.py

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import Optional
from db.connection import get_db_connection
from passlib.context import CryptContext

router = APIRouter(prefix="/users", tags=["Users"])
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

class UserCreate(BaseModel):
    username: str
    password: str
    role: str = "viewer"

class PasswordChange(BaseModel):
    user_id: int
    new_password: str

async def get_current_user_id(request: Request) -> int:
    """从请求头提取当前用户ID"""
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="未授权")
    
    token = auth_header[7:]
    parts = token.split(".")
    if len(parts) < 3:
        raise HTTPException(status_code=401, detail="无效token")
    
    username = parts[1]
    with get_db_connection() as con:
        user = con.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
        if not user:
            raise HTTPException(status_code=401, detail="用户不存在")
        return user[0]

@router.get("")
def list_users():
    with get_db_connection() as con:
        users = con.execute("SELECT id, username, role, CAST(created_at AS VARCHAR) FROM users").fetchall()
        return [{"id": u[0], "username": u[1], "role": u[2], "created_at": u[3]} for u in users]

@router.post("")
def create_user(user: UserCreate):
    hashed_password = pwd_context.hash(user.password)
    try:
        with get_db_connection() as con:
            con.execute("INSERT INTO users (username, hashed_password, role) VALUES (?, ?, ?)", (user.username, hashed_password, user.role))
        return {"message": f"用户 {user.username} 创建成功"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"创建用户失败: {e}")

@router.delete("/{user_id}")
def delete_user(user_id: int):
    with get_db_connection() as con:
        con.execute("DELETE FROM users WHERE id = ?", (user_id,))
    return {"message": "用户已删除"}

@router.put("/password")
def change_password(data: PasswordChange):
    hashed_password = pwd_context.hash(data.new_password)
    with get_db_connection() as con:
        con.execute("UPDATE users SET hashed_password = ? WHERE id = ?", (hashed_password, data.user_id))
    return {"message": "密码修改成功"}