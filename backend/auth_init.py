"""
One-Time Setup Script: Initialize Authentication System

This script:
1. Creates new tables: user_credentials, superusers, password_reset_tokens
2. Creates initial superuser account
3. Auto-generates credentials for all existing DEOs with password "password123"

Run this ONCE after adding the new models to your database.

Usage:
    python initialize_auth_system.py
"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from sqlalchemy.orm import Session
from app.models.database import engine, SessionLocal
from app.models import models
from app.auth.auth_utils import get_password_hash
from datetime import datetime

def create_tables():
    """Create new authentication tables"""
    print("=" * 80)
    print("STEP 1: Creating new authentication tables...")
    print("=" * 80)
    
    # This will create ONLY the new tables (existing ones won't be affected)
    models.Base.metadata.create_all(bind=engine)
    
    print("✅ Tables created successfully")
    print("   - user_credentials")
    print("   - superusers")
    print("   - password_reset_tokens")
    print()


def create_initial_superuser(db: Session):
    """Create the first superuser account"""
    print("=" * 80)
    print("STEP 2: Creating initial superuser...")
    print("=" * 80)
    
    # Check if any superuser exists
    existing = db.query(models.Superuser).first()
    
    if existing:
        print("⚠️  Superuser already exists. Skipping...")
        return
    
    # Create superuser profile
    superuser = models.Superuser(
        full_name="System Administrator",
        email="admin@bsk.gov.in",
        phone="1234567890",
        role="super_admin",
        is_active=True,
        created_at=datetime.now()
    )
    db.add(superuser)
    db.flush()
    
    # Create credentials
    default_password = "admin123"  # ⚠️ CHANGE THIS IN PRODUCTION!
    
    creds = models.UserCredentials(
        user_id=superuser.superuser_id,
        user_type="superuser",
        username="admin",
        password_hash=get_password_hash(default_password),
        must_change_password=True,
        is_active=True,
        created_at=datetime.now()
    )
    db.add(creds)
    db.commit()
    
    print("✅ Initial superuser created:")
    print(f"   Username: admin")
    print(f"   Password: {default_password}")
    print(f"   Email: admin@bsk.gov.in")
    print(f"   ⚠️  IMPORTANT: Change this password after first login!")
    print()


def initialize_deo_credentials(db: Session):
    """Create credentials for all existing DEOs with password 'password123'"""
    print("=" * 80)
    print("STEP 3: Initializing DEO credentials...")
    print("=" * 80)
    
    # Get all active DEOs
    deos = db.query(models.DEOMaster).filter(
        models.DEOMaster.is_active == True
    ).all()
    
    print(f"Found {len(deos)} active DEOs")
    
    created_count = 0
    skipped_count = 0
    
    default_password = "password123"
    
    for deo in deos:
        # Check if credentials already exist
        existing = db.query(models.UserCredentials).filter(
            models.UserCredentials.user_id == deo.agent_id,
            models.UserCredentials.user_type == "deo"
        ).first()
        
        if existing:
            skipped_count += 1
            continue
        
        # Create credentials
        creds = models.UserCredentials(
            user_id=deo.agent_id,
            user_type="deo",
            username=deo.agent_code,
            password_hash=get_password_hash(default_password),
            must_change_password=True,  # Force password change
            is_active=True,
            created_at=datetime.now()
        )
        db.add(creds)
        created_count += 1
    
    db.commit()
    
    print(f"✅ DEO credentials initialized:")
    print(f"   Created: {created_count}")
    print(f"   Skipped (already exist): {skipped_count}")
    print(f"   Default password: {default_password}")
    print(f"   DEOs will be forced to change password on first login")
    print()


def verify_setup(db: Session):
    """Verify the setup was successful"""
    print("=" * 80)
    print("VERIFICATION: Checking setup...")
    print("=" * 80)
    
    # Check superusers
    superuser_count = db.query(models.Superuser).count()
    print(f"✅ Superusers: {superuser_count}")
    
    # Check DEO credentials
    deo_creds_count = db.query(models.UserCredentials).filter(
        models.UserCredentials.user_type == "deo"
    ).count()
    print(f"✅ DEO credentials: {deo_creds_count}")
    
    # Check total credentials
    total_creds = db.query(models.UserCredentials).count()
    print(f"✅ Total credentials: {total_creds}")
    print()


def main():
    """Main setup function"""
    print()
    print("=" * 80)
    print("BSK TRAINING API - AUTHENTICATION SYSTEM INITIALIZATION")
    print("=" * 80)
    print()
    
    # Create database session
    db = SessionLocal()
    
    try:
        # Step 1: Create tables
        create_tables()
        
        # Step 2: Create initial superuser
        create_initial_superuser(db)
        
        # Step 3: Initialize DEO credentials
        initialize_deo_credentials(db)
        
        # Step 4: Verify setup
        verify_setup(db)
        
        print("=" * 80)
        print("✅ AUTHENTICATION SYSTEM INITIALIZED SUCCESSFULLY!")
        print("=" * 80)
        print()
        print("📝 NEXT STEPS:")
        print("   1. Login as superuser:")
        print("      Username: admin")
        print("      Password: admin123")
        print()
        print("   2. Change superuser password immediately!")
        print()
        print("   3. DEOs can login with:")
        print("      Username: <their agent_code>")
        print("      Password: password123")
        print()
        print("   4. DEOs will be forced to change password on first login")
        print()
        print("=" * 80)
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        db.rollback()
        raise
    
    finally:
        db.close()


if __name__ == "__main__":
    main()