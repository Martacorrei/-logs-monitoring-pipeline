import ast
import json
import logging
import re
from typing import List, Dict, Tuple, Optional

import pandas as pd

from src.docgenius.logs_doc_utils import process_doc_msg
from src.general_utils import GeneralUtils


# Configure logging
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


async def read_log_file(file_path: str) -> List[str]:
    """
    Read log file and return lines, handling empty files.

    Parameters
    ----------
    file_path : str
        Path to the log file to read

    Returns
    -------
    List[str]
        Lines from the log file or empty list if file is empty/error occurs

    Notes
    -----
    Uses UTF-8 encoding for file reading
    """
    try:
        with open(file_path, 'r', encoding="utf-8") as file:
            lines = file.readlines()
        
        if len(lines) == 0:
            logging.info("No logs to process")
            return []
        
        logging.info(f"Successfully read log file: {len(lines)}")
        return lines
    except Exception as e:
        logging.error(f"Error reading log file: {e}")
        return []


def extract_log_entries(lines: List[str]) -> List[str]:
    """
    Extract log entries from raw lines.

    Parameters
    ----------
    lines : List[str]
        Raw log lines to process

    Returns
    -------
    List[str]
        Extracted and processed log entries

    Notes
    -----
    Removes 'stdout F' and 'stderr F' prefixes from log entries
    """
    extracted_logs = []
    
    for line in lines:
        line = line.strip()
        if not line:  # Ignore empty lines
            continue
        
        try:
            log_entry = ast.literal_eval(line)
            json_entry = json.loads(json.dumps(log_entry))
            
            if isinstance(json_entry, dict) and "log" in json_entry:
                extracted_logs.append(json_entry["log"])
            else:
                logging.warning(f"JSON entry does not contain 'log' key: {line}")
        
        except (SyntaxError, ValueError) as e:
            logging.error(f"Erro ao processar JSON na linha: {line}\nErro: {e}")
    
    # Remove "stdout F " and "stderr F " prefixes
    extracted_logs = [log.replace("stdout F ", "").replace("stderr F ", "") for log in extracted_logs]
    logging.info(f"Successfully extracted and processed logs: {len(extracted_logs)}")
    return extracted_logs


def structure_logs(extracted_logs: List[str]) -> pd.DataFrame:
    """
    Structure logs using regex pattern into a DataFrame.

    Parameters
    ----------
    extracted_logs : List[str]
        List of extracted log entries

    Returns
    -------
    pd.DataFrame
        Structured logs with processed columns

    Notes
    -----
    Processes timestamps, durations, and additional message details
    """
    structured_logs = []

    log_pattern = re.compile(
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - "  # (1) Timestamp
        r"([\w\.\-]+) - "                            # (2) Module
        r"(\w+) - "                                  # (3) Level
        r"(\[unknown\] \[DOC_[a-f0-9\-]+\]|\[DOC_[a-f0-9\-]+\]|\[REQ_[a-f0-9\-]+\] \[unknown\]|\[REQ_[a-f0-9\-]+\]) " # (4) Req id/ doc id
        r"(.+)"                                      # (5) Message
    )

    for line in extracted_logs:
        match = log_pattern.search(line)
        if match:
            log_entry = _create_log_entry(match)
            structured_logs.append(log_entry)

    return _create_structured_dataframe(structured_logs)


def _create_log_entry(match: re.Match) -> Dict:
    """
    Create a log entry dictionary from regex match.

    Parameters
    ----------
    match : re.Match
        Regex match object containing log components

    Returns
    -------
    Dict
        Processed log entry dictionary
    """
    log_entry = {
        "timestamp": match.group(1),
        "module": match.group(2),
        "level": match.group(3),
        "id": match.group(4),
        "msg": match.group(5),
    }
    
    parts = log_entry["msg"].split(" | ")
    log_entry["msg"] = parts[0]

    for part in parts[1:]:
        if ": " in part:
            key, value = part.split(": ", 1)
            log_entry[key.strip().lower().replace(" ", "_")] = value.strip()
    return log_entry


def _create_structured_dataframe(structured_logs: List[Dict]) -> pd.DataFrame:
    """
    Create and process structured DataFrame from log entries.

    Parameters
    ----------
    structured_logs : List[Dict]
        List of structured log entry dictionaries

    Returns
    -------
    pd.DataFrame
        Processed DataFrame with converted data types
    """
    df_logs = pd.DataFrame(structured_logs)

    if not df_logs.empty:
        if "timestamp" in df_logs.columns:
            df_logs["timestamp"] = pd.to_datetime(df_logs["timestamp"], errors="coerce")
        if "duration_(s)" in df_logs.columns:
            df_logs["duration_(s)"] = pd.to_numeric(df_logs["duration_(s)"], errors="coerce")
        if "duration" in df_logs.columns:
            df_logs["duration"] = pd.to_numeric(df_logs["duration"], errors="coerce")
    
    df_logs.rename(columns={"duration_(s)": "duration_s"}, inplace=True)

    logging.info(f"Successfully structured logs: {len(df_logs)}")
    logging.info(f"Columns: {df_logs.columns.tolist()}")
    return df_logs


def separate_req_and_doc_logs(df_logs: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Separate request and document logs into separate DataFrames.

    Parameters
    ----------
    df_logs : pd.DataFrame
        Combined logs DataFrame

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame]
        (request_logs, document_logs) DataFrames

    Notes
    -----
    Processes request paths and applies document message processing
    """
    if df_logs.empty:
        logging.info("No logs to separate")
        return pd.DataFrame(), pd.DataFrame()
    
    df_logs = df_logs.copy()
    df_logs.loc[:, 'id'] = df_logs['id'].str.replace(r'\[unknown\]', '', regex=True).str.strip()

    df_req_logs = _process_request_logs(df_logs[df_logs['id'].str.startswith('[REQ')].copy())
    df_doc_logs = _process_document_logs(df_logs[~df_logs['id'].str.startswith('[REQ')].copy())

    logging.info(f"Successfully separated logs - Requests: {len(df_req_logs)}, Documents: {len(df_doc_logs)}")
    return df_req_logs, df_doc_logs


def _process_request_logs(df_req_logs: pd.DataFrame) -> pd.DataFrame:
    """
    Process request-specific logs.

    Parameters
    ----------
    df_req_logs : pd.DataFrame
        Request logs DataFrame

    Returns
    -------
    pd.DataFrame
        Processed request logs with generalized paths
    """
    df_req_logs = df_req_logs.rename(columns={'id': 'req_id'})
    
    # Substitute project and invoice IDs in paths
    df_req_logs.loc[:, 'path_edited'] = df_req_logs['path'].str.replace(
        r'/api/v1/projects/[a-zA-Z0-9\-]+', 
        '/api/v1/projects/{project_id}', 
        regex=True
    ).str.replace(
        r'/api/v1/projects/{project_id}/invoices/[a-zA-Z0-9\-]+', 
        '/api/v1/projects/{project_id}/invoices/{invoice_id}', 
        regex=True
    )
    return df_req_logs


def _process_document_logs(df_doc_logs: pd.DataFrame) -> pd.DataFrame:
    """
    Process document-specific logs.

    Parameters
    ----------
    df_doc_logs : pd.DataFrame
        Document logs DataFrame

    Returns
    -------
    pd.DataFrame
        Processed document logs
    """
    df_doc_logs = df_doc_logs.rename(columns={'id': 'doc_id'})
    df_doc_logs = df_doc_logs.dropna(axis=1, how='all')
    return df_doc_logs.apply(process_doc_msg, axis=1)


async def save_logs_to_database(
    df_req_logs: pd.DataFrame,
    df_doc_logs: pd.DataFrame,
    df_req_errors: pd.DataFrame,
    df_req_login: pd.DataFrame,
    df_req_path_duration: pd.DataFrame,
    df_doc_errors: pd.DataFrame,
    df_doc_time: pd.DataFrame,
    df_doc_logs_duration: pd.DataFrame
) -> Optional[Dict[str, int]]:
    """
    Save processed logs to PostgreSQL database.

    Parameters
    ----------
    df_req_logs : pd.DataFrame
        Request logs DataFrame
    df_doc_logs : pd.DataFrame
        Document logs DataFrame
    df_req_errors : pd.DataFrame
        Request errors DataFrame
    df_req_login : pd.DataFrame
        Request login DataFrame
    df_req_path_duration : pd.DataFrame
        Request path duration DataFrame
    df_doc_errors : pd.DataFrame
        Document errors DataFrame
    df_doc_time : pd.DataFrame
        Document time DataFrame
    df_doc_logs_duration : pd.DataFrame
        Document logs duration DataFrame

    Returns
    -------
    Optional[Dict[str, int]]
        Dictionary with table names and row counts, or None if error occurs

    Notes
    -----
    Uses transaction to ensure all-or-nothing save operation
    """

    utils= GeneralUtils()
    dict_df_name={
        'req_logs': df_req_logs,
        'doc_logs': df_doc_logs,
        'req_errors': df_req_errors,
        'req_login': df_req_login,
        'req_path_duration': df_req_path_duration,
        'doc_errors': df_doc_errors,
        'doc_time': df_doc_time,
        'doc_logs_duration': df_doc_logs_duration
    }

    conn = await utils.get_db_connection('db_docgenius')
    transaction = conn.transaction()
    
    try:
        await transaction.start()
        result = {}

        for table_name, df in dict_df_name.items(): 
            if not df.empty:
                logging.info(f"Saving {len(df)} rows to {table_name}...")
                await utils.docgenius_append_to_db('db_docgenius', df, table_name, 'logs', conn=conn)
                result[table_name] = len(df)
            else:
                logging.info(f"No data to save for {table_name}")
                result[table_name] = 0
        
        # If all operations were successful, commit the transaction
        await transaction.commit()
        logging.info("Successfully saved all data in transaction")
        return result
    
    except Exception as e:
        await transaction.rollback()
        logging.error(f"Database save error: {e}")
        return None
    
    finally:
        await conn.close()
