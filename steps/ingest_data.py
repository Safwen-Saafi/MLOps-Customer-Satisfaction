import logging
import os
import pandas as pd
from zenml import step


class IngestData:
    """
    Data ingestion class which ingests data from the source and returns a DataFrame.
    """

    def __init__(self) -> None:
        """Initialize the data ingestion class."""
        pass

    def get_data(self) -> pd.DataFrame:
        file_path = "./data/olist_customers_dataset.csv"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Data file not found at {file_path}")
        df = pd.read_csv(file_path)
        return df


@step
def ingest_data() -> pd.DataFrame:
    """
    Ingest data from the source.
    
    Args:
        None
    
    Returns:
        pd.DataFrame: DataFrame containing the ingested data
    """
    try:
        logging.info("Starting data ingestion step.")
        ingest_data = IngestData()
        df = ingest_data.get_data()
        logging.info("Data ingestion successful.")
        return df
    except FileNotFoundError as fnf_error:
        logging.error(f"File not found: {fnf_error}")
        raise fnf_error
    except pd.errors.ParserError as parse_error:
        logging.error(f"Parsing error: {parse_error}")
        raise parse_error
    except Exception as e:
        logging.error(f"An error occurred during data ingestion: {e}")
        raise e
