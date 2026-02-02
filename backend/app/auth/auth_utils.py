# # # ============================================================================
# # # NEW FILE: app/auth/auth_utils.py
# # # ============================================================================
# # """
# # Authentication and Authorization utilities for BSK Training API

# # This module provides:
# # 1. JWT token generation and validation
# # 2. Role-based access control (RBAC)
# # 3. User authentication helpers
# # """

# # import os
# # from datetime import datetime, timedelta
# # from typing import Optional, Dict, Any
# # from fastapi import Depends, HTTPException, status, Header
# # from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
# # from jose import JWTError, jwt
# # from passlib.context import CryptContext
# # from sqlalchemy.orm import Session
# # from pydantic import BaseModel

# # # ============================================================================
# # # CONFIGURATION
# # # ============================================================================

# # # Get from environment variables in production
# # SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-change-in-production")
# # ALGORITHM = "HS256"
# # ACCESS_TOKEN_EXPIRE_MINUTES = 480  # 8 hours

# # pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
# # security = HTTPBearer()

# # # ============================================================================
# # # PYDANTIC MODELS
# # # ============================================================================

# # class TokenData(BaseModel):
# #     """Data stored in JWT token"""
# #     user_id: int
# #     role: str  # "superuser" or "deo"
# #     bsk_id: Optional[int] = None  # Only for DEO users
# #     agent_code: Optional[str] = None  # DEO agent code


# # class UserAuth(BaseModel):
# #     """User authentication request"""
# #     username: str  # Can be agent_code for DEO or admin username
# #     password: str


# # class TokenResponse(BaseModel):
# #     """JWT token response"""
# #     access_token: str
# #     token_type: str = "bearer"
# #     role: str
# #     bsk_id: Optional[int] = None


# # # ============================================================================
# # # PASSWORD HASHING
# # # ============================================================================

# # def verify_password(plain_password: str, hashed_password: str) -> bool:
# #     """Verify a plain password against hashed password"""
# #     return pwd_context.verify(plain_password, hashed_password)


# # def get_password_hash(password: str) -> str:
# #     """Hash a password"""
# #     return pwd_context.hash(password)


# # # ============================================================================
# # # JWT TOKEN MANAGEMENT
# # # ============================================================================

# # def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
# #     """
# #     Create JWT access token
    
# #     Args:
# #         data: Dictionary containing user info (user_id, role, bsk_id)
# #         expires_delta: Token expiration time (default: 8 hours)
    
# #     Returns:
# #         JWT token string
# #     """
# #     to_encode = data.copy()
    
# #     if expires_delta:
# #         expire = datetime.utcnow() + expires_delta
# #     else:
# #         expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
# #     to_encode.update({"exp": expire})
# #     encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    
# #     return encoded_jwt


# # def decode_access_token(token: str) -> TokenData:
# #     """
# #     Decode and validate JWT token
    
# #     Args:
# #         token: JWT token string
    
# #     Returns:
# #         TokenData with user info
    
# #     Raises:
# #         HTTPException: If token is invalid or expired
# #     """
# #     credentials_exception = HTTPException(
# #         status_code=status.HTTP_401_UNAUTHORIZED,
# #         detail="Could not validate credentials",
# #         headers={"WWW-Authenticate": "Bearer"},
# #     )
    
# #     try:
# #         payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
# #         user_id: int = payload.get("user_id")
# #         role: str = payload.get("role")
        
# #         if user_id is None or role is None:
# #             raise credentials_exception
        
# #         return TokenData(
# #             user_id=user_id,
# #             role=role,
# #             bsk_id=payload.get("bsk_id"),
# #             agent_code=payload.get("agent_code")
# #         )
    
# #     except JWTError:
# #         raise credentials_exception


# # # ============================================================================
# # # AUTHENTICATION DEPENDENCIES
# # # ============================================================================

# # async def get_current_user(
# #     credentials: HTTPAuthorizationCredentials = Depends(security)
# # ) -> TokenData:
# #     """
# #     Dependency to get current authenticated user from JWT token
    
# #     Usage in endpoint:
# #         @app.get("/protected")
# #         def protected_route(user: TokenData = Depends(get_current_user)):
# #             ...
# #     """
# #     token = credentials.credentials
# #     return decode_access_token(token)


# # async def require_superuser(
# #     user: TokenData = Depends(get_current_user)
# # ) -> TokenData:
# #     """
# #     Dependency to require superuser role
    
# #     Usage:
# #         @app.get("/admin-only")
# #         def admin_route(user: TokenData = Depends(require_superuser)):
# #             ...
# #     """
# #     if user.role != "superuser":
# #         raise HTTPException(
# #             status_code=status.HTTP_403_FORBIDDEN,
# #             detail="Superuser access required"
# #         )
# #     return user


# # async def require_deo(
# #     user: TokenData = Depends(get_current_user)
# # ) -> TokenData:
# #     """
# #     Dependency to require DEO role
    
# #     Usage:
# #         @app.get("/deo-only")
# #         def deo_route(user: TokenData = Depends(require_deo)):
# #             ...
# #     """
# #     if user.role != "deo":
# #         raise HTTPException(
# #             status_code=status.HTTP_403_FORBIDDEN,
# #             detail="DEO access required"
# #         )
# #     return user


# # # ============================================================================
# # # USER AUTHENTICATION LOGIC
# # # ============================================================================

# # def authenticate_deo(db: Session, agent_code: str, password: str) -> Optional[Dict[str, Any]]:
# #     """
# #     Authenticate a DEO user
    
# #     Args:
# #         db: Database session
# #         agent_code: DEO agent code
# #         password: Plain password
    
# #     Returns:
# #         Dict with user info if authenticated, None otherwise
# #     """
# #     from app.models import models  # Import here to avoid circular dependency
    
# #     # Find DEO by agent_code
# #     deo = db.query(models.DEOMaster).filter(
# #         models.DEOMaster.agent_code == agent_code,
# #         models.DEOMaster.is_active == True
# #     ).first()
    
# #     if not deo:
# #         return None
    
# #     # ========================================================================
# #     # TEMPORARY PASSWORD VALIDATION (FOR TESTING/DEMO)
# #     # ========================================================================
# #     # Option 1: Accept any password (easiest for testing)
# #     # Just comment out the password check below
    
# #     # Option 2: Use default password "password123" for all DEOs
# #     DEFAULT_PASSWORD = "password123"
# #     if password != DEFAULT_PASSWORD:
# #         return None
    
# #     # Option 3: Use agent_code as password (e.g., agent_code="DEO001", password="DEO001")
# #     # if password != agent_code:
# #     #     return None
    
# #     # ========================================================================
# #     # PRODUCTION: Replace above with real password verification
# #     # ========================================================================
# #     # creds = db.query(models.UserCredentials).filter(
# #     #     models.UserCredentials.user_id == deo.agent_id
# #     # ).first()
# #     # if not creds or not verify_password(password, creds.password_hash):
# #     #     return None
    
# #     return {
# #         "user_id": deo.agent_id,
# #         "role": "deo",
# #         "bsk_id": deo.bsk_id,
# #         "agent_code": deo.agent_code,
# #         "user_name": deo.user_name
# #     }


# # def authenticate_superuser(username: str, password: str) -> Optional[Dict[str, Any]]:
# #     """
# #     Authenticate a superuser (admin)
    
# #     Args:
# #         username: Admin username
# #         password: Plain password
    
# #     Returns:
# #         Dict with user info if authenticated, None otherwise
# #     """
# #     # HARDCODED SUPERUSER FOR DEMO
# #     # In production, fetch from a separate admin_users table
    
# #     SUPERUSER_CREDENTIALS = {
# #         "admin": "admin123",  # Change in production!
# #         "superadmin": "super123"
# #     }
    
# #     if username in SUPERUSER_CREDENTIALS and SUPERUSER_CREDENTIALS[username] == password:
# #         return {
# #             "user_id": 1,  # Admin user ID
# #             "role": "superuser",
# #             "username": username
# #         }
    
# #     return None


# # # ============================================================================
# # # AUTHORIZATION HELPERS
# # # ============================================================================

# # def filter_recommendations_by_role(
# #     recommendations: list,
# #     user: TokenData
# # ) -> list:
# #     """
# #     Filter training recommendations based on user role
    
# #     Args:
# #         recommendations: Full list of recommendations
# #         user: Current authenticated user
    
# #     Returns:
# #         Filtered recommendations (all for superuser, single BSK for DEO)
# #     """
# #     if user.role == "superuser":
# #         # Superuser sees everything
# #         return recommendations
    
# #     elif user.role == "deo":
# #         # DEO sees only their assigned BSK
# #         if user.bsk_id is None:
# #             return []  # DEO without BSK assignment
        
# #         # Filter to show only this DEO's BSK
# #         return [rec for rec in recommendations if rec.get("bsk_id") == user.bsk_id]
    
# #     return []  # Unknown role


# # def check_bsk_access(user: TokenData, requested_bsk_id: int) -> bool:
# #     """
# #     Check if user has access to a specific BSK
    
# #     Args:
# #         user: Current authenticated user
# #         requested_bsk_id: BSK ID being requested
    
# #     Returns:
# #         True if access allowed, False otherwise
# #     """
# #     if user.role == "superuser":
# #         return True  # Superuser has access to all BSKs
    
# #     elif user.role == "deo":
# #         return user.bsk_id == requested_bsk_id  # DEO can only access their BSK
    
# #     return False





# # ============================================================================
# # ENHANCED FILE: app/auth/auth_utils.py
# # ============================================================================
# """
# Database-backed Authentication and Authorization for BSK Training API

# Features:
# - ✅ Database-stored credentials (not hardcoded)
# - ✅ Bcrypt password hashing
# - ✅ Default password "password123" for new DEOs
# - ✅ Password change capability
# - ✅ Account lockout after failed attempts
# - ✅ Last login tracking
# """

# import os
# import secrets
# from datetime import datetime, timedelta
# from typing import Optional, Dict, Any
# from fastapi import Depends, HTTPException, status
# from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
# from jose import JWTError, jwt
# from passlib.context import CryptContext
# from sqlalchemy.orm import Session
# from pydantic import BaseModel
# import logging

# logger = logging.getLogger(__name__)

# # ============================================================================
# # CONFIGURATION
# # ============================================================================

# SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-change-in-production")
# ALGORITHM = "HS256"
# ACCESS_TOKEN_EXPIRE_MINUTES = 480  # 8 hours

# # Password policy
# MAX_FAILED_ATTEMPTS = 5
# LOCKOUT_DURATION_MINUTES = 30
# DEFAULT_DEO_PASSWORD = "password123"

# pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
# security = HTTPBearer()

# # ============================================================================
# # PYDANTIC MODELS
# # ============================================================================

# class TokenData(BaseModel):
#     """Data stored in JWT token"""
#     user_id: int
#     role: str  # "superuser" or "deo"
#     bsk_id: Optional[int] = None
#     agent_code: Optional[str] = None
#     must_change_password: bool = False


# class UserAuth(BaseModel):
#     """User authentication request"""
#     username: str
#     password: str


# class TokenResponse(BaseModel):
#     """JWT token response"""
#     access_token: str
#     token_type: str = "bearer"
#     role: str
#     bsk_id: Optional[int] = None
#     must_change_password: bool = False


# class PasswordChange(BaseModel):
#     """Password change request"""
#     current_password: str
#     new_password: str


# # ============================================================================
# # PASSWORD HASHING
# # ============================================================================

# def verify_password(plain_password: str, hashed_password: str) -> bool:
#     """Verify a plain password against hashed password"""
#     return pwd_context.verify(plain_password, hashed_password)


# def get_password_hash(password: str) -> str:
#     """Hash a password using bcrypt"""
#     return pwd_context.hash(password)


# # ============================================================================
# # JWT TOKEN MANAGEMENT
# # ============================================================================

# def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
#     """Create JWT access token"""
#     to_encode = data.copy()
    
#     if expires_delta:
#         expire = datetime.utcnow() + expires_delta
#     else:
#         expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
#     to_encode.update({"exp": expire})
#     encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    
#     return encoded_jwt


# def decode_access_token(token: str) -> TokenData:
#     """Decode and validate JWT token"""
#     credentials_exception = HTTPException(
#         status_code=status.HTTP_401_UNAUTHORIZED,
#         detail="Could not validate credentials",
#         headers={"WWW-Authenticate": "Bearer"},
#     )
    
#     try:
#         payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
#         user_id: int = payload.get("user_id")
#         role: str = payload.get("role")
        
#         if user_id is None or role is None:
#             raise credentials_exception
        
#         return TokenData(
#             user_id=user_id,
#             role=role,
#             bsk_id=payload.get("bsk_id"),
#             agent_code=payload.get("agent_code"),
#             must_change_password=payload.get("must_change_password", False)
#         )
    
#     except JWTError:
#         raise credentials_exception


# # ============================================================================
# # AUTHENTICATION DEPENDENCIES
# # ============================================================================

# async def get_current_user(
#     credentials: HTTPAuthorizationCredentials = Depends(security)
# ) -> TokenData:
#     """Get current authenticated user from JWT token"""
#     token = credentials.credentials
#     return decode_access_token(token)


# async def require_superuser(
#     user: TokenData = Depends(get_current_user)
# ) -> TokenData:
#     """Require superuser role"""
#     if user.role != "superuser":
#         raise HTTPException(
#             status_code=status.HTTP_403_FORBIDDEN,
#             detail="Superuser access required"
#         )
#     return user


# # ============================================================================
# # DATABASE-BACKED AUTHENTICATION
# # ============================================================================

# def check_account_lockout(creds, db: Session) -> bool:
#     """
#     Check if account is locked due to failed login attempts.
    
#     Returns True if locked, False if okay to proceed.
#     """
#     if creds.account_locked_until:
#         if datetime.now() < creds.account_locked_until:
#             # Still locked
#             remaining_minutes = (creds.account_locked_until - datetime.now()).seconds // 60
#             raise HTTPException(
#                 status_code=status.HTTP_423_LOCKED,
#                 detail=f"Account locked due to multiple failed login attempts. Try again in {remaining_minutes} minutes."
#             )
#         else:
#             # Lockout expired - reset
#             creds.account_locked_until = None
#             creds.failed_login_attempts = 0
#             db.commit()
    
#     return False


# def handle_failed_login(creds, db: Session):
#     """Handle failed login attempt - increment counter and lock if needed"""
#     creds.failed_login_attempts += 1
    
#     if creds.failed_login_attempts >= MAX_FAILED_ATTEMPTS:
#         creds.account_locked_until = datetime.now() + timedelta(minutes=LOCKOUT_DURATION_MINUTES)
#         logger.warning(f"🔒 Account locked: {creds.username} (too many failed attempts)")
    
#     db.commit()


# def handle_successful_login(creds, db: Session):
#     """Handle successful login - reset counters and update last login"""
#     creds.failed_login_attempts = 0
#     creds.account_locked_until = None
#     creds.last_login = datetime.now()
#     db.commit()


# def authenticate_deo(db: Session, agent_code: str, password: str) -> Optional[Dict[str, Any]]:
#     """
#     Authenticate a DEO user using database credentials.
    
#     Process:
#     1. Check if DEO exists in deo_master
#     2. Check if credentials exist in user_credentials
#     3. If no credentials, auto-create with default password "password123"
#     4. Verify password
#     5. Check account lockout status
#     6. Return user info if authenticated
#     """
#     from app.models import models
    
#     # Step 1: Find DEO in master table
#     deo = db.query(models.DEOMaster).filter(
#         models.DEOMaster.agent_code == agent_code,
#         models.DEOMaster.is_active == True
#     ).first()
    
#     if not deo:
#         logger.warning(f"❌ DEO not found: {agent_code}")
#         return None
    
#     # Step 2: Get or create credentials
#     creds = db.query(models.UserCredentials).filter(
#         models.UserCredentials.user_id == deo.agent_id,
#         models.UserCredentials.user_type == "deo"
#     ).first()
    
#     if not creds:
#         # Auto-create credentials with default password
#         logger.info(f"✨ Creating default credentials for DEO: {agent_code}")
#         creds = models.UserCredentials(
#             user_id=deo.agent_id,
#             user_type="deo",
#             username=agent_code,
#             password_hash=get_password_hash(DEFAULT_DEO_PASSWORD),
#             must_change_password=True,  # Force password change on first login
#             is_active=True
#         )
#         db.add(creds)
#         db.commit()
#         db.refresh(creds)
    
#     # Step 3: Check if account is active
#     if not creds.is_active:
#         raise HTTPException(
#             status_code=status.HTTP_403_FORBIDDEN,
#             detail="Account is deactivated. Contact administrator."
#         )
    
#     # Step 4: Check account lockout
#     check_account_lockout(creds, db)
    
#     # Step 5: Verify password
#     if not verify_password(password, creds.password_hash):
#         logger.warning(f"❌ Invalid password for DEO: {agent_code}")
#         handle_failed_login(creds, db)
#         return None
    
#     # Step 6: Successful login
#     logger.info(f"✅ DEO authenticated: {agent_code} (BSK: {deo.bsk_id})")
#     handle_successful_login(creds, db)
    
#     return {
#         "user_id": deo.agent_id,
#         "role": "deo",
#         "bsk_id": deo.bsk_id,
#         "agent_code": deo.agent_code,
#         "user_name": deo.user_name,
#         "must_change_password": creds.must_change_password
#     }


# def authenticate_superuser(db: Session, username: str, password: str) -> Optional[Dict[str, Any]]:
#     """
#     Authenticate a superuser using database credentials.
    
#     Process:
#     1. Find superuser in superusers table
#     2. Get credentials from user_credentials
#     3. Verify password
#     4. Check account status
#     5. Return user info if authenticated
#     """
#     from app.models import models
    
#     # Step 1: Find credentials by username
#     creds = db.query(models.UserCredentials).filter(
#         models.UserCredentials.username == username,
#         models.UserCredentials.user_type == "superuser"
#     ).first()
    
#     if not creds:
#         logger.warning(f"❌ Superuser not found: {username}")
#         return None
    
#     # Step 2: Get superuser profile
#     superuser = db.query(models.Superuser).filter(
#         models.Superuser.superuser_id == creds.user_id,
#         models.Superuser.is_active == True
#     ).first()
    
#     if not superuser:
#         logger.warning(f"❌ Superuser profile not found or inactive: {username}")
#         return None
    
#     # Step 3: Check account status
#     if not creds.is_active:
#         raise HTTPException(
#             status_code=status.HTTP_403_FORBIDDEN,
#             detail="Account is deactivated. Contact system administrator."
#         )
    
#     # Step 4: Check account lockout
#     check_account_lockout(creds, db)
    
#     # Step 5: Verify password
#     if not verify_password(password, creds.password_hash):
#         logger.warning(f"❌ Invalid password for superuser: {username}")
#         handle_failed_login(creds, db)
#         return None
    
#     # Step 6: Successful login
#     logger.info(f"✅ Superuser authenticated: {username} ({superuser.role})")
#     handle_successful_login(creds, db)
    
#     return {
#         "user_id": superuser.superuser_id,
#         "role": "superuser",
#         "username": username,
#         "full_name": superuser.full_name,
#         "must_change_password": creds.must_change_password
#     }


# # ============================================================================
# # PASSWORD MANAGEMENT
# # ============================================================================

# def change_password(
#     db: Session,
#     user_id: int,
#     user_type: str,
#     current_password: str,
#     new_password: str
# ) -> bool:
#     """
#     Change user password.
    
#     Args:
#         db: Database session
#         user_id: User ID (agent_id or superuser_id)
#         user_type: "deo" or "superuser"
#         current_password: Current password (for verification)
#         new_password: New password to set
    
#     Returns:
#         True if password changed successfully
    
#     Raises:
#         HTTPException: If current password is incorrect
#     """
#     from app.models import models
    
#     # Get credentials
#     creds = db.query(models.UserCredentials).filter(
#         models.UserCredentials.user_id == user_id,
#         models.UserCredentials.user_type == user_type
#     ).first()
    
#     if not creds:
#         raise HTTPException(status_code=404, detail="User credentials not found")
    
#     # Verify current password
#     if not verify_password(current_password, creds.password_hash):
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail="Current password is incorrect"
#         )
    
#     # Validate new password
#     if len(new_password) < 8:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail="New password must be at least 8 characters"
#         )
    
#     # Update password
#     creds.password_hash = get_password_hash(new_password)
#     creds.password_last_changed = datetime.now()
#     creds.must_change_password = False  # Remove force-change flag
#     db.commit()
    
#     logger.info(f"✅ Password changed for {user_type}: {creds.username}")
#     return True


# # ============================================================================
# # AUTHORIZATION HELPERS
# # ============================================================================

# def filter_recommendations_by_role(
#     recommendations: list,
#     user: TokenData
# ) -> list:
#     """Filter training recommendations based on user role"""
#     if user.role == "superuser":
#         return recommendations
    
#     elif user.role == "deo":
#         if user.bsk_id is None:
#             return []
#         return [rec for rec in recommendations if rec.get("bsk_id") == user.bsk_id]
    
#     return []


# def check_bsk_access(user: TokenData, requested_bsk_id: int) -> bool:
#     """Check if user has access to a specific BSK"""
#     if user.role == "superuser":
#         return True
    
#     elif user.role == "deo":
#         return user.bsk_id == requested_bsk_id
    
#     return False




# ============================================================================
# FILE: app/auth/auth_utils.py
# ============================================================================
"""
Authentication and Authorization utilities for BSK Training API

UPDATED VERSION - Uses agent_email as unique identifier instead of agent_code

This module provides:
1. JWT token generation and validation
2. Role-based access control (RBAC)
3. User authentication helpers
4. Password management
5. Account lockout protection
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy.orm import Session
from pydantic import BaseModel, EmailStr

# ============================================================================
# LOGGING SETUP
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 480  # 8 hours

# Password policy
MIN_PASSWORD_LENGTH = 8
MAX_FAILED_ATTEMPTS = 5
LOCKOUT_DURATION_MINUTES = 30
DEFAULT_DEO_PASSWORD = "password123"

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer()

# ============================================================================
# PYDANTIC MODELS
# ============================================================================

class TokenData(BaseModel):
    """Data stored in JWT token"""
    user_id: int
    role: str  # "superuser" or "deo"
    bsk_id: Optional[int] = None
    agent_email: Optional[str] = None  # DEO agent email (unique identifier)
    username: Optional[str] = None  # For superusers
    must_change_password: bool = False


class UserAuth(BaseModel):
    """User authentication request"""
    username: str  # agent_email for DEO, username for superuser
    password: str


class TokenResponse(BaseModel):
    """JWT token response"""
    access_token: str
    token_type: str = "bearer"
    role: str
    bsk_id: Optional[int] = None
    must_change_password: bool = False


class PasswordChange(BaseModel):
    """Password change request"""
    current_password: str
    new_password: str


# ============================================================================
# PASSWORD HASHING
# ============================================================================

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a plain password against hashed password"""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """Hash a password using bcrypt"""
    return pwd_context.hash(password)


# ============================================================================
# JWT TOKEN MANAGEMENT
# ============================================================================

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """
    Create JWT access token
    
    Args:
        data: Dictionary containing user info (user_id, role, bsk_id, agent_email)
        expires_delta: Token expiration time (default: 8 hours)
    
    Returns:
        JWT token string
    """
    to_encode = data.copy()
    
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    
    return encoded_jwt


def decode_access_token(token: str) -> TokenData:
    """
    Decode and validate JWT token
    
    Args:
        token: JWT token string
    
    Returns:
        TokenData with user info
    
    Raises:
        HTTPException: If token is invalid or expired
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: int = payload.get("user_id")
        role: str = payload.get("role")
        
        if user_id is None or role is None:
            raise credentials_exception
        
        return TokenData(
            user_id=user_id,
            role=role,
            bsk_id=payload.get("bsk_id"),
            agent_email=payload.get("agent_email"),  # Updated from agent_code
            username=payload.get("username"),
            must_change_password=payload.get("must_change_password", False)
        )
    
    except JWTError:
        raise credentials_exception


# ============================================================================
# AUTHENTICATION DEPENDENCIES
# ============================================================================

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> TokenData:
    """
    Dependency to get current authenticated user from JWT token
    
    Usage in endpoint:
        @app.get("/protected")
        def protected_route(user: TokenData = Depends(get_current_user)):
            ...
    """
    token = credentials.credentials
    return decode_access_token(token)


async def require_superuser(
    user: TokenData = Depends(get_current_user)
) -> TokenData:
    """
    Dependency to require superuser role
    
    Usage:
        @app.get("/admin-only")
        def admin_route(user: TokenData = Depends(require_superuser)):
            ...
    """
    if user.role != "superuser":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Superuser access required"
        )
    return user


async def require_deo(
    user: TokenData = Depends(get_current_user)
) -> TokenData:
    """
    Dependency to require DEO role
    
    Usage:
        @app.get("/deo-only")
        def deo_route(user: TokenData = Depends(require_deo)):
            ...
    """
    if user.role != "deo":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="DEO access required"
        )
    return user


# ============================================================================
# ACCOUNT LOCKOUT PROTECTION
# ============================================================================

def check_account_lockout(creds, db: Session):
    """Check if account is locked due to failed login attempts"""
    if creds.account_locked_until and creds.account_locked_until > datetime.now():
        time_remaining = (creds.account_locked_until - datetime.now()).seconds // 60
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Account locked due to too many failed login attempts. Try again in {time_remaining} minutes."
        )
    elif creds.account_locked_until and creds.account_locked_until <= datetime.now():
        # Unlock account
        creds.account_locked_until = None
        creds.failed_login_attempts = 0
        db.commit()


def handle_failed_login(creds, db: Session):
    """Handle failed login attempt - increment counter and lock if needed"""
    creds.failed_login_attempts += 1
    
    if creds.failed_login_attempts >= MAX_FAILED_ATTEMPTS:
        creds.account_locked_until = datetime.now() + timedelta(minutes=LOCKOUT_DURATION_MINUTES)
        logger.warning(f"🔒 Account locked: {creds.username} (too many failed attempts)")
    
    db.commit()


def handle_successful_login(creds, db: Session):
    """Handle successful login - reset counters and update last login"""
    creds.failed_login_attempts = 0
    creds.account_locked_until = None
    creds.last_login = datetime.now()
    db.commit()


# ============================================================================
# DEO AUTHENTICATION (UPDATED TO USE agent_email)
# ============================================================================

def authenticate_deo(db: Session, agent_email: str, password: str) -> Optional[Dict[str, Any]]:
    """
    Authenticate a DEO user using agent_email as identifier.
    
    Process:
    1. Check if DEO exists in deo_master by agent_email
    2. Check if credentials exist in user_credentials
    3. If no credentials, auto-create with default password "password123"
    4. Verify password
    5. Check account lockout status
    6. Return user info if authenticated
    
    Args:
        db: Database session
        agent_email: DEO agent email (unique identifier)
        password: Plain password
    
    Returns:
        Dict with user info if authenticated, None otherwise
    """
    from app.models import models
    
    # Step 1: Find DEO in master table by agent_email
    deo = db.query(models.DEOMaster).filter(
        models.DEOMaster.agent_email == agent_email,  # Updated from agent_code
        models.DEOMaster.is_active == True
    ).first()
    
    if not deo:
        logger.warning(f"❌ DEO not found with email: {agent_email}")
        return None
    
    # Step 2: Get or create credentials
    creds = db.query(models.UserCredentials).filter(
        models.UserCredentials.user_id == deo.agent_id,
        models.UserCredentials.user_type == "deo"
    ).first()
    
    if not creds:
        # Auto-create credentials with default password
        logger.info(f"✨ Creating default credentials for DEO: {agent_email}")
        creds = models.UserCredentials(
            user_id=deo.agent_id,
            user_type="deo",
            username=agent_email,  # Use email as username
            password_hash=get_password_hash(DEFAULT_DEO_PASSWORD),
            must_change_password=True,  # Force password change on first login
            is_active=True
        )
        db.add(creds)
        db.commit()
        db.refresh(creds)
    
    # Step 3: Check if account is active
    if not creds.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is deactivated. Contact administrator."
        )
    
    # Step 4: Check account lockout
    check_account_lockout(creds, db)
    
    # Step 5: Verify password
    if not verify_password(password, creds.password_hash):
        logger.warning(f"❌ Invalid password for DEO: {agent_email}")
        handle_failed_login(creds, db)
        return None
    
    # Step 6: Successful login
    logger.info(f"✅ DEO authenticated: {agent_email} (BSK: {deo.bsk_id})")
    handle_successful_login(creds, db)
    
    return {
        "user_id": deo.agent_id,
        "role": "deo",
        "bsk_id": deo.bsk_id,
        "agent_email": deo.agent_email,  # Updated from agent_code
        "user_name": deo.user_name,
        "must_change_password": creds.must_change_password
    }


# ============================================================================
# SUPERUSER AUTHENTICATION
# ============================================================================

def authenticate_superuser(db: Session, username: str, password: str) -> Optional[Dict[str, Any]]:
    """
    Authenticate a superuser using database credentials.
    
    Process:
    1. Find superuser in superusers table
    2. Get credentials from user_credentials
    3. Verify password
    4. Check account status
    5. Return user info if authenticated
    """
    from app.models import models
    
    # Step 1: Find credentials by username
    creds = db.query(models.UserCredentials).filter(
        models.UserCredentials.username == username,
        models.UserCredentials.user_type == "superuser"
    ).first()
    
    if not creds:
        logger.warning(f"❌ Superuser not found: {username}")
        return None
    
    # Step 2: Get superuser profile
    superuser = db.query(models.Superuser).filter(
        models.Superuser.superuser_id == creds.user_id,
        models.Superuser.is_active == True
    ).first()
    
    if not superuser:
        logger.warning(f"❌ Superuser profile not found or inactive: {username}")
        return None
    
    # Step 3: Check account status
    if not creds.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is deactivated. Contact system administrator."
        )
    
    # Step 4: Check account lockout
    check_account_lockout(creds, db)
    
    # Step 5: Verify password
    if not verify_password(password, creds.password_hash):
        logger.warning(f"❌ Invalid password for superuser: {username}")
        handle_failed_login(creds, db)
        return None
    
    # Step 6: Successful login
    logger.info(f"✅ Superuser authenticated: {username} ({superuser.role})")
    handle_successful_login(creds, db)
    
    return {
        "user_id": superuser.superuser_id,
        "role": "superuser",
        "username": username,
        "full_name": superuser.full_name,
        "must_change_password": creds.must_change_password
    }


# ============================================================================
# PASSWORD MANAGEMENT
# ============================================================================

def change_password(
    db: Session,
    user_id: int,
    user_type: str,
    current_password: str,
    new_password: str
) -> bool:
    """
    Change user password.
    
    Args:
        db: Database session
        user_id: User ID (agent_id or superuser_id)
        user_type: "deo" or "superuser"
        current_password: Current password (for verification)
        new_password: New password to set
    
    Returns:
        True if password changed successfully
    
    Raises:
        HTTPException: If current password is incorrect
    """
    from app.models import models
    
    # Get credentials
    creds = db.query(models.UserCredentials).filter(
        models.UserCredentials.user_id == user_id,
        models.UserCredentials.user_type == user_type
    ).first()
    
    if not creds:
        raise HTTPException(status_code=404, detail="User credentials not found")
    
    # Verify current password
    if not verify_password(current_password, creds.password_hash):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Current password is incorrect"
        )
    
    # Validate new password
    if len(new_password) < MIN_PASSWORD_LENGTH:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"New password must be at least {MIN_PASSWORD_LENGTH} characters"
        )
    
    # Update password
    creds.password_hash = get_password_hash(new_password)
    creds.password_last_changed = datetime.now()
    creds.must_change_password = False  # Remove force-change flag
    db.commit()
    
    logger.info(f"✅ Password changed for {user_type}: {creds.username}")
    return True


# ============================================================================
# AUTHORIZATION HELPERS
# ============================================================================

def filter_recommendations_by_role(
    recommendations: list,
    user: TokenData
) -> list:
    """Filter training recommendations based on user role"""
    if user.role == "superuser":
        return recommendations
    
    elif user.role == "deo":
        if user.bsk_id is None:
            return []
        return [rec for rec in recommendations if rec.get("bsk_id") == user.bsk_id]
    
    return []


def check_bsk_access(user: TokenData, requested_bsk_id: int) -> bool:
    """Check if user has access to a specific BSK"""
    if user.role == "superuser":
        return True
    
    elif user.role == "deo":
        return user.bsk_id == requested_bsk_id
    
    return False