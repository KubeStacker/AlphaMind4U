"""
用户认证模块
"""
from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
import bcrypt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy import text
from db.database import get_db
import logging

logger = logging.getLogger(__name__)

# 使用bcrypt进行密码加密

# JWT配置
import os
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-change-in-production-use-env-variable")  # 生产环境应使用环境变量
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 480  # 8小时

# 安全配置
MAX_LOGIN_ATTEMPTS = 5  # 最大登录尝试次数
LOCKOUT_DURATION_MINUTES = 30  # 锁定持续时间（分钟）

def get_lockout_duration():
    """获取锁定持续时间"""
    return timedelta(minutes=LOCKOUT_DURATION_MINUTES)

# HTTP Bearer认证
security = HTTPBearer()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """验证密码"""
    try:
        if not plain_password or not hashed_password:
            logger.warning("密码验证失败: 密码或哈希值为空")
            return False
        # 确保哈希值是bytes格式
        if isinstance(hashed_password, str):
            hashed_bytes = hashed_password.encode('utf-8')
        else:
            hashed_bytes = hashed_password
        return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_bytes)
    except Exception as e:
        logger.error(f"密码验证异常: {e}", exc_info=True)
        return False


def get_password_hash(password: str) -> str:
    """生成密码哈希"""
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """创建JWT访问令牌"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def authenticate_user(username: str, password: str, ip_address: str = None, user_agent: str = None) -> Optional[dict]:
    """验证用户凭据"""
    with get_db() as db:
        # 检查用户是否存在且未锁定
        query = text("""
            SELECT id, username, password_hash, is_active, failed_login_attempts, locked_until
            FROM users
            WHERE username = :username
        """)
        result = db.execute(query, {"username": username})
        user = result.fetchone()
        
        if not user:
            # 记录失败的登录尝试
            log_login_attempt(db, username, ip_address, user_agent, False, "用户不存在")
            return None
        
        user_id, db_username, password_hash, is_active, failed_attempts, locked_until = user
        
        # 检查账户是否被锁定
        if locked_until:
            try:
                if isinstance(locked_until, datetime):
                    lock_time = locked_until
                else:
                    lock_time = datetime.fromisoformat(str(locked_until))
                if datetime.now() < lock_time:
                    log_login_attempt(db, username, ip_address, user_agent, False, "账户已锁定")
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail="账户已被锁定，请稍后再试"
                    )
            except (ValueError, TypeError):
                pass  # 如果时间格式错误，继续验证
        
        # 检查账户是否激活
        if not is_active:
            log_login_attempt(db, username, ip_address, user_agent, False, "账户未激活")
            return None
        
        # 验证密码
        if not verify_password(password, password_hash):
            # 增加失败次数
            failed_attempts += 1
            lock_duration = None
            
            # 如果失败次数达到上限，锁定账户
            if failed_attempts >= MAX_LOGIN_ATTEMPTS:
                lock_duration = datetime.now() + get_lockout_duration()
            
            update_query = text("""
                UPDATE users
                SET failed_login_attempts = :attempts,
                    locked_until = :locked_until
                WHERE id = :user_id
            """)
            db.execute(update_query, {
                "attempts": failed_attempts,
                "locked_until": lock_duration,
                "user_id": user_id
            })
            db.commit()
            
            log_login_attempt(db, username, ip_address, user_agent, False, "密码错误")
            
            if failed_attempts >= MAX_LOGIN_ATTEMPTS:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"密码错误次数过多，账户已被锁定{LOCKOUT_DURATION_MINUTES}分钟"
                )
            
            return None
        
        # 登录成功，重置失败次数和锁定时间
        update_query = text("""
            UPDATE users
            SET failed_login_attempts = 0,
                locked_until = NULL,
                last_login = NOW()
            WHERE id = :user_id
        """)
        db.execute(update_query, {"user_id": user_id})
        db.commit()
        
        # 记录成功登录
        log_login_attempt(db, username, ip_address, user_agent, True, None)
        
        return {
            "id": user_id,
            "username": db_username
        }


def log_login_attempt(db, username: str, ip_address: str, user_agent: str, success: bool, failure_reason: str = None):
    """记录登录尝试"""
    try:
        query = text("""
            INSERT INTO login_logs (username, ip_address, user_agent, login_status, failure_reason)
            VALUES (:username, :ip_address, :user_agent, :status, :reason)
        """)
        db.execute(query, {
            "username": username,
            "ip_address": ip_address,
            "user_agent": user_agent,
            "status": "success" if success else "failed",
            "reason": failure_reason
        })
        db.commit()
    except Exception as e:
        logger.error(f"记录登录日志失败: {e}")


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    """从JWT令牌获取当前用户"""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="无法验证凭据",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        token = credentials.credentials
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    
    # 验证用户是否仍然存在且激活
    with get_db() as db:
        query = text("""
            SELECT id, username, is_active
            FROM users
            WHERE username = :username
        """)
        result = db.execute(query, {"username": username})
        user = result.fetchone()
        
        if not user or not user[2]:  # is_active
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="用户不存在或已被禁用"
            )
        
        return {
            "id": user[0],
            "username": user[1]
        }
