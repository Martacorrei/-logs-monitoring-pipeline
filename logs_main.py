import logging
from typing import Tuple, Optional

import pandas as pd

from src.docgenius.logs_utils import (
    read_log_file,
    extract_log_entries,
    structure_logs,
    separate_req_and_doc_logs,
    save_logs_to_database,
)
from src.docgenius.logs_req_utils import (
    process_request_errors,
    process_request_path_duration,
    process_request_login,
)
from src.docgenius.logs_doc_utils import (
    process_document_errors,
    process_document_time,
    process_document_duration,
)
from src.docgenius.email_utils import send_email_docgenius


# Configure logging
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


async def process_logs_with_email(file_path: str) -> None:
    """
    Process logs and send results via email.

    Parameters
    ----------
    file_path : str
        Path to the log file to process
    """
    result = await _process_logs(file_path)
    send_email_docgenius(result)


def _process_request_logs(df_req_logs: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Process request-specific logs.

    Parameters
    ----------
    df_req_logs : pd.DataFrame
        DataFrame containing request logs

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
        Tuple containing processed DataFrames for errors, logins, and path durations
    """
    if df_req_logs.empty:
        logging.info("No request logs to process")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    df_req_errors = process_request_errors(df_req_logs)
    df_req_login = process_request_login(df_req_logs)
    df_req_path_duration = process_request_path_duration(df_req_logs)

    return df_req_errors, df_req_login, df_req_path_duration


def _process_document_logs(df_doc_logs: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Process document-specific logs.

    Parameters
    ----------
    df_doc_logs : pd.DataFrame
        DataFrame containing document logs

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
        Tuple containing processed DataFrames for errors, processing time, and duration
    """
    if df_doc_logs.empty:
        logging.info("No document logs to process")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    df_doc_errors = process_document_errors(df_doc_logs)
    df_doc_time = process_document_time(df_doc_logs)
    df_doc_logs_duration = process_document_duration(df_doc_logs)

    return df_doc_errors, df_doc_time, df_doc_logs_duration


async def _process_logs(file_path: str) -> Optional[bool]:
    """
    Main log processing function.

    Parameters
    ----------
    file_path : str
        Path to the log file to process

    Returns
    -------
    Optional[bool]
        True if processing was successful, None if there was an error

    Notes
    -----
    This function orchestrates the entire log processing workflow:
    1. Reads and extracts log entries
    2. Structures the logs into DataFrames
    3. Processes different types of logs
    4. Saves results to database
    """
    try:
        # Read and extract logs
        lines = await read_log_file(file_path)
        if not lines:
            logging.info("No logs to process")
            return None

        extracted_logs = extract_log_entries(lines)
        if not extracted_logs:
            logging.info("No logs extracted")
            return None

        # Structure logs into DataFrame
        df_logs = structure_logs(extracted_logs)
        if df_logs.empty:
            logging.info("No logs structured")
            return None

        # Separate and process logs
        df_req_logs, df_doc_logs = separate_req_and_doc_logs(df_logs)
        
        df_req_errors, df_req_login, df_req_path_duration = _process_request_logs(df_req_logs)
        df_doc_errors, df_doc_time, df_doc_logs_duration = _process_document_logs(df_doc_logs)

        # Save processed results
        return await save_logs_to_database(
            df_req_logs, df_doc_logs,
            df_req_errors, df_req_login, df_req_path_duration,
            df_doc_errors, df_doc_time, df_doc_logs_duration
        )
    
    except Exception as e:
        logging.error(f"Error processing logs: {e}")
        return None
