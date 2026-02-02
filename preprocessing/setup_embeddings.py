"""
Setup script for initializing service embeddings with ChromaDB.
Run this script after installing dependencies to set up the vector database.
"""

import sys
import os

# Add the project directories to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from ai_service.database_service import (
        fetch_services_from_db,
        test_database_connection,
    )
    from ai_service.service_recommendation import (
        initialize_service_embeddings,
        get_embedding_stats,
    )
except ImportError as e:
    print(f"Import error: {e}")
    print("Please ensure you're running this script from the project root directory")
    print("and that all dependencies are installed.")
    print("\nTo install dependencies, run:")
    print("pip install -r requirements_embeddings.txt")
    sys.exit(1)

"""
Setup script for initializing service embeddings with ChromaDB.
Run this script after installing dependencies to set up the vector database.
"""

import sys
import os

# Add the project directories to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from ai_service.database_service import (
        fetch_services_from_db,
        test_database_connection,
    )
    from ai_service.service_recommendation import (
        initialize_service_embeddings,
        get_embedding_stats,
    )
except ImportError as e:
    print(f"Import error: {e}")
    print("Please ensure you're running this script from the project root directory")
    print("and that all dependencies are installed.")
    print("\nTo install dependencies, run:")
    print("pip install -r requirements_embeddings.txt")
    sys.exit(1)


def setup_embeddings():
    """Setup service embeddings from ServiceMaster database table."""
    print("Setting up service embeddings from ServiceMaster database...")

    # Test database connection first
    print("Testing database connection...")
    if not test_database_connection():
        print("❌ Database connection failed!")
        print("\nPlease check:")
        print("1. Database is running and accessible")
        print("2. .env file contains correct DATABASE_URL")
        print("3. Database credentials are correct")
        return False

    print("✅ Database connection successful!")

    try:
        # Load services data from database
        print("\nFetching services from ServiceMaster table...")
        services_df = fetch_services_from_db(include_inactive=False)

        if services_df is None or services_df.empty:
            print("❌ Failed to load services data from database")
            print("Please check if the ServiceMaster table has data")
            return False

        print(f"✅ Successfully loaded {len(services_df)} active services")

        # Display some sample data
        print("\nSample services:")
        for idx, row in services_df.head(3).iterrows():
            service_name = str(row["service_name"])[:50]
            service_type = str(row.get("service_type", "N/A"))
            print(
                f"  - ID: {row['service_id']}, Name: {service_name}..., Type: {service_type}"
            )

        # Show column information
        print(f"\nAvailable columns: {list(services_df.columns)}")

        # Initialize embeddings
        print("\nInitializing embeddings...")
        initialize_service_embeddings(services_df, force_rebuild=True)

        # Show stats
        stats = get_embedding_stats()
        print(f"\n✅ Setup complete!")
        print(f"📊 Total services with embeddings: {stats['total_services']}")
        print(f"📁 Database location: {stats['db_path']}")
        print(f"🗂️  Collection name: {stats['collection_name']}")

        return True

    except Exception as e:
        print(f"❌ Error during setup: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("Service Embedding Setup")
    print("=" * 50)

    success = setup_embeddings()

    if success:
        print("\n✅ Setup completed successfully!")
        print("You can now use the optimized service recommendation system.")
    else:
        print("\n❌ Setup failed. Please check the errors above and try again.")
        sys.exit(1)
