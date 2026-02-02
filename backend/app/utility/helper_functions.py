from typing import List
import pandas as pd
from sqlalchemy.orm import Session
from app.models import models
import logging

logger = logging.getLogger(__name__)


def convert_models_to_dataframe(model_list: List) -> pd.DataFrame:
    """
    Convert a list of SQLAlchemy model instances to a pandas DataFrame.
    This utility function handles the conversion of ORM objects to a DataFrame
    and removes SQLAlchemy's internal state tracking column.
    Args:
        model_list: List of SQLAlchemy model instances
    Returns:
        pd.DataFrame: DataFrame with model data, excluding internal state
    """
    df = pd.DataFrame([item.__dict__ for item in model_list])
    # Remove SQLAlchemy internal state column if present
    if "_sa_instance_state" in df.columns:
        df.drop("_sa_instance_state", axis=1, inplace=True)
    return df


def fetch_all_master_data(db: Session) -> tuple:
    """
    Fetch all master data from database and convert to DataFrames.
    This function retrieves all records from the four main tables:
    BSK Master, Provisions, DEO Master, and Service Master, and converts
    them to pandas DataFrames for analytics processing.
    Args:
        db: SQLAlchemy database session
    Returns:
        tuple: (bsks_df, provisions_df, deos_df, services_df)
    """
    logger.info("Fetching all master data from database...")

    # Retrieve all records from each table
    bsks = db.query(models.BSKMaster).all()
    provisions = db.query(models.Provision).all()
    deos = db.query(models.DEOMaster).all()
    services = db.query(models.ServiceMaster).all()

    logger.info(
        f"Retrieved {len(bsks)} BSKs, {len(provisions)} provisions, "
        f"{len(deos)} DEOs, {len(services)} services"
    )

    # Convert to DataFrames and clean up
    bsks_df = convert_models_to_dataframe(bsks)
    provisions_df = convert_models_to_dataframe(provisions)
    deos_df = convert_models_to_dataframe(deos)
    services_df = convert_models_to_dataframe(services)

    return bsks_df, provisions_df, deos_df, services_df
