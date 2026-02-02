"""
Database Initialization Script for BSK Training API
====================================================

This script performs first-time setup:
1. Creates user_credentials table if it doesn't exist
2. Initializes passwords for all DEOs (default: "password123")
3. Initializes passwords for all superusers (default: "admin123")
4. Can be run multiple times safely (idempotent)

Usage:
    python init_db_passwords.py

Requirements:
    - Database connection must be configured
    - deo_master table must exist with DEOs
    - superusers table must exist with superusers
"""

import sys
import os
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from sqlalchemy.orm import Session
from sqlalchemy import text, inspect
from datetime import datetime
import logging


from models.database import engine, SessionLocal
from models import models
from auth.auth_utils import get_password_hash

# ============================================================================
# CONFIGURATION
# ============================================================================

DEFAULT_DEO_PASSWORD = "password123"
DEFAULT_SUPERUSER_PASSWORD = "admin123"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def table_exists(table_name: str) -> bool:
    """Check if a table exists in the database"""
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()


def create_tables():
    """Create all tables if they don't exist"""
    logger.info("📋 Checking database tables...")
    
    # Create all tables defined in models
    models.Base.metadata.create_all(bind=engine)
    
    logger.info("✅ All tables verified/created")


def initialize_deo_passwords(db: Session) -> dict:
    """
    Initialize passwords for all DEOs in deo_master table
    
    Returns:
        dict with statistics
    """
    logger.info("🔐 Initializing DEO passwords...")
    
    stats = {
        "total_deos": 0,
        "already_had_credentials": 0,
        "created_credentials": 0,
        "skipped_inactive": 0,
        "missing_email": 0,
        "errors": 0
    }
    
    # Get all DEOs
    deos = db.query(models.DEOMaster).all()
    stats["total_deos"] = len(deos)
    
    logger.info(f"📊 Found {len(deos)} DEOs in deo_master table")
    
    for deo in deos:
        try:
            # Skip inactive DEOs
            if not deo.is_active:
                stats["skipped_inactive"] += 1
                logger.debug(f"⏭️  Skipping inactive DEO: {deo.agent_id}")
                continue
            
            # Check if DEO has email
            if not deo.agent_email or deo.agent_email.strip() == "":
                stats["missing_email"] += 1
                logger.warning(
                    f"⚠️  DEO {deo.agent_id} ({deo.user_name}) has no email address. "
                    f"Skipping credential creation."
                )
                continue
            
            # Check if credentials already exist
            existing_creds = db.query(models.UserCredentials).filter(
                models.UserCredentials.user_id == deo.agent_id,
                models.UserCredentials.user_type == "deo"
            ).first()
            
            if existing_creds:
                stats["already_had_credentials"] += 1
                logger.debug(
                    f"✓ DEO {deo.agent_email} already has credentials"
                )
                continue
            
            # Create new credentials
            new_creds = models.UserCredentials(
                user_id=deo.agent_id,
                user_type="deo",
                username=deo.agent_email,  # Use email as username
                password_hash=get_password_hash(DEFAULT_DEO_PASSWORD),
                must_change_password=True,  # Force password change on first login
                is_active=True,
                created_at=datetime.now(),
                password_last_changed=datetime.now()
            )
            
            db.add(new_creds)
            stats["created_credentials"] += 1
            
            logger.info(
                f"✅ Created credentials for DEO: {deo.agent_email} "
                f"(ID: {deo.agent_id}, Name: {deo.user_name})"
            )
            
        except Exception as e:
            stats["errors"] += 1
            logger.error(f"❌ Error processing DEO {deo.agent_id}: {e}")
    
    # Commit all changes
    try:
        db.commit()
        logger.info("💾 DEO credentials committed to database")
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Failed to commit DEO credentials: {e}")
        raise
    
    return stats


def initialize_superuser_passwords(db: Session) -> dict:
    """
    Initialize passwords for all superusers
    
    Returns:
        dict with statistics
    """
    logger.info("🔐 Initializing superuser passwords...")
    
    stats = {
        "total_superusers": 0,
        "already_had_credentials": 0,
        "created_credentials": 0,
        "skipped_inactive": 0,
        "missing_username": 0,
        "errors": 0
    }
    
    # Get all superusers
    superusers = db.query(models.Superuser).all()
    stats["total_superusers"] = len(superusers)
    
    logger.info(f"📊 Found {len(superusers)} superusers in superusers table")
    
    for superuser in superusers:
        try:
            # Skip inactive superusers
            if not superuser.is_active:
                stats["skipped_inactive"] += 1
                logger.debug(f"⏭️  Skipping inactive superuser: {superuser.superuser_id}")
                continue
            
            # Generate username if not exists (use email or create from full_name)
            username = superuser.email if hasattr(superuser, 'email') and superuser.email else None
            
            if not username:
                # Fallback: create username from full_name
                if superuser.full_name:
                    username = superuser.full_name.lower().replace(" ", "_")
                else:
                    username = f"superuser_{superuser.superuser_id}"
            
            if not username or username.strip() == "":
                stats["missing_username"] += 1
                logger.warning(
                    f"⚠️  Superuser {superuser.superuser_id} has no username/email. "
                    f"Skipping credential creation."
                )
                continue
            
            # Check if credentials already exist
            existing_creds = db.query(models.UserCredentials).filter(
                models.UserCredentials.user_id == superuser.superuser_id,
                models.UserCredentials.user_type == "superuser"
            ).first()
            
            if existing_creds:
                stats["already_had_credentials"] += 1
                logger.debug(
                    f"✓ Superuser {username} already has credentials"
                )
                continue
            
            # Create new credentials
            new_creds = models.UserCredentials(
                user_id=superuser.superuser_id,
                user_type="superuser",
                username=username,
                password_hash=get_password_hash(DEFAULT_SUPERUSER_PASSWORD),
                must_change_password=True,  # Force password change on first login
                is_active=True,
                created_at=datetime.now(),
                password_last_changed=datetime.now()
            )
            
            db.add(new_creds)
            stats["created_credentials"] += 1
            
            logger.info(
                f"✅ Created credentials for superuser: {username} "
                f"(ID: {superuser.superuser_id}, Name: {superuser.full_name})"
            )
            
        except Exception as e:
            stats["errors"] += 1
            logger.error(f"❌ Error processing superuser {superuser.superuser_id}: {e}")
    
    # Commit all changes
    try:
        db.commit()
        logger.info("💾 Superuser credentials committed to database")
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Failed to commit superuser credentials: {e}")
        raise
    
    return stats


def print_summary(deo_stats: dict, superuser_stats: dict):
    """Print summary of initialization"""
    
    print("\n" + "="*70)
    print("📊 DATABASE INITIALIZATION SUMMARY")
    print("="*70)
    
    print("\n🔵 DEO CREDENTIALS:")
    print(f"   Total DEOs found:           {deo_stats['total_deos']}")
    print(f"   ✅ Created credentials:      {deo_stats['created_credentials']}")
    print(f"   ✓  Already had credentials: {deo_stats['already_had_credentials']}")
    print(f"   ⏭️  Skipped (inactive):       {deo_stats['skipped_inactive']}")
    print(f"   ⚠️  Missing email:           {deo_stats['missing_email']}")
    print(f"   ❌ Errors:                   {deo_stats['errors']}")
    
    print("\n🟢 SUPERUSER CREDENTIALS:")
    print(f"   Total superusers found:     {superuser_stats['total_superusers']}")
    print(f"   ✅ Created credentials:      {superuser_stats['created_credentials']}")
    print(f"   ✓  Already had credentials: {superuser_stats['already_had_credentials']}")
    print(f"   ⏭️  Skipped (inactive):       {superuser_stats['skipped_inactive']}")
    print(f"   ⚠️  Missing username:        {superuser_stats['missing_username']}")
    print(f"   ❌ Errors:                   {superuser_stats['errors']}")
    
    print("\n" + "="*70)
    print("🔑 DEFAULT PASSWORDS:")
    print("="*70)
    print(f"   DEO Password:        {DEFAULT_DEO_PASSWORD}")
    print(f"   Superuser Password:  {DEFAULT_SUPERUSER_PASSWORD}")
    print("\n⚠️  All users MUST change their password on first login!")
    print("="*70 + "\n")


def verify_initialization(db: Session):
    """Verify that initialization was successful"""
    
    logger.info("🔍 Verifying initialization...")
    
    # Count credentials
    deo_creds_count = db.query(models.UserCredentials).filter(
        models.UserCredentials.user_type == "deo"
    ).count()
    
    superuser_creds_count = db.query(models.UserCredentials).filter(
        models.UserCredentials.user_type == "superuser"
    ).count()
    
    # Count active users
    active_deos = db.query(models.DEOMaster).filter(
        models.DEOMaster.is_active == True
    ).count()
    
    active_superusers = db.query(models.Superuser).filter(
        models.Superuser.is_active == True
    ).count()
    
    print("\n" + "="*70)
    print("✅ VERIFICATION RESULTS:")
    print("="*70)
    print(f"   Active DEOs in system:           {active_deos}")
    print(f"   DEO credentials in database:     {deo_creds_count}")
    print(f"   Active superusers in system:     {active_superusers}")
    print(f"   Superuser credentials in DB:     {superuser_creds_count}")
    print("="*70 + "\n")


def fix_missing_emails(db: Session):
    """
    Auto-generate email addresses for DEOs that don't have them
    Format: deo{agent_id}@bangla.gov.in
    """
    logger.info("🔧 Checking for DEOs without email addresses...")
    
    deos_without_email = db.query(models.DEOMaster).filter(
        (models.DEOMaster.agent_email == None) | 
        (models.DEOMaster.agent_email == "")
    ).all()
    
    if not deos_without_email:
        logger.info("✅ All DEOs have email addresses")
        return 0
    
    logger.warning(f"⚠️  Found {len(deos_without_email)} DEOs without email addresses")
    
    fixed_count = 0
    for deo in deos_without_email:
        # Generate email: deo{id}@bangla.gov.in
        generated_email = f"deo{deo.agent_id}@bangla.gov.in"
        deo.agent_email = generated_email
        fixed_count += 1
        logger.info(f"✅ Generated email for DEO {deo.agent_id}: {generated_email}")
    
    db.commit()
    logger.info(f"💾 Generated {fixed_count} email addresses")
    
    return fixed_count


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    """Main initialization function"""
    
    print("\n" + "="*70)
    print("🚀 BSK TRAINING API - DATABASE INITIALIZATION")
    print("="*70 + "\n")
    
    try:
        # Step 1: Create tables
        logger.info("Step 1: Creating database tables...")
        create_tables()
        
        # Step 2: Create database session
        logger.info("\nStep 2: Connecting to database...")
        db = SessionLocal()
        
        # Step 3: Fix missing emails
        logger.info("\nStep 3: Checking DEO email addresses...")
        fixed_emails = fix_missing_emails(db)
        if fixed_emails > 0:
            logger.info(f"✅ Auto-generated {fixed_emails} email addresses")
        
        # Step 4: Initialize DEO passwords
        logger.info("\nStep 4: Initializing DEO credentials...")
        deo_stats = initialize_deo_passwords(db)
        
        # Step 5: Initialize superuser passwords
        logger.info("\nStep 5: Initializing superuser credentials...")
        superuser_stats = initialize_superuser_passwords(db)
        
        # Step 6: Verify
        logger.info("\nStep 6: Verifying initialization...")
        verify_initialization(db)
        
        # Step 7: Print summary
        print_summary(deo_stats, superuser_stats)
        
        # Success message
        total_created = deo_stats['created_credentials'] + superuser_stats['created_credentials']
        
        if total_created > 0:
            print("🎉 SUCCESS! Database initialization complete!")
            print(f"   Created {total_created} new user credentials")
            print("\n📝 NEXT STEPS:")
            print("   1. Start your application: uvicorn main:app --reload")
            print("   2. Test DEO login:")
            print(f"      - Email: (check logs above)")
            print(f"      - Password: {DEFAULT_DEO_PASSWORD}")
            print("   3. Test superuser login:")
            print(f"      - Username: (check logs above)")
            print(f"      - Password: {DEFAULT_SUPERUSER_PASSWORD}")
            print("   4. Users must change password on first login\n")
        else:
            print("ℹ️  All users already have credentials.")
            print("   No new credentials were created.\n")
        
        db.close()
        
    except Exception as e:
        logger.error(f"❌ FATAL ERROR during initialization: {e}")
        print("\n❌ Initialization failed! See error above.")
        import traceback
        traceback.print_exc()
        sys.exit(1)


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    main()