import os
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests
import pandas as pd
from dotenv import load_dotenv

from src.general_utils import GeneralUtils


# Configure logging
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

load_dotenv()
KIBANA_URL: str = os.getenv("KIBANA_URL")
KIBANA_API_KEY: str = os.getenv("KIBANA_API_KEY")
HEADERS = {
    "Authorization": f"ApiKey {KIBANA_API_KEY}",
    "Content-Type": "application/json",
    "kbn-xsrf": "true"
}

DEFAULT_START_DATE = "2025-02-21T00:00:00.000Z"
BATCH_SIZE = 5000
LOG_DIR = "/tmp"


async def _convert_to_utc_timestamp(date: datetime) -> str:
    """
    Convert a datetime object to UTC and format it for Kibana.

    Parameters
    ----------
    date : datetime
        The datetime object to convert

    Returns
    -------
    str
        Formatted UTC timestamp string in Kibana format (YYYY-MM-DDThh:mm:ss.SSSZ)

    Notes
    -----
    If the input datetime has no timezone info, UTC+1 is assumed
    """
    if date.tzinfo is None:
        logging.info("Date has no timezone info. Assuming UTC+1")
        local_tz = timezone(timedelta(hours=1))
        date = date.replace(tzinfo=local_tz)
    
    date_utc = date.astimezone(timezone.utc)
    return date_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


async def _get_max_timestamp(df: pd.DataFrame) -> Optional[str]:
    """
    Extract and format the maximum timestamp from a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing a 'timestamp' column

    Returns
    -------
    str or None
        Formatted UTC timestamp string if successful, None otherwise
    """
    try:
        max_date = df['timestamp'].max()
        if isinstance(max_date, str):
            max_date = datetime.fromisoformat(max_date)
        elif isinstance(max_date, pd.Timestamp):
            max_date = max_date.to_pydatetime()
        
        return await _convert_to_utc_timestamp(max_date)
    except Exception as e:
        logging.error(f"Error processing maximum date: {e}")
        return None


async def _get_start_date() -> str:
    """
    Determine the start date for Kibana log extraction.

    Returns
    -------
    str
        Formatted UTC timestamp string for Kibana query start date

    Notes
    -----
    Returns DEFAULT_START_DATE if no previous data is found or in case of errors
    """
    try:
        logging.info("Getting start date...")
        utils = GeneralUtils()
        df_timestamp = await utils.get_last_date_docgenius()
        
        if not df_timestamp.empty:
            formatted_date = await _get_max_timestamp(df_timestamp)
            if formatted_date:
                return formatted_date
    except Exception as e:
        logging.error(f"Error getting start date: {e}")
    
    return DEFAULT_START_DATE


def _create_kibana_query(start_date: str, search_after: Optional[list] = None) -> dict:
    """
    Create Kibana query parameters.

    Parameters
    ----------
    start_date : str
        Start date for the query in Kibana format
    search_after : list, optional
        Search after value for pagination

    Returns
    -------
    dict
        Kibana query parameters
    """
    query = {
        "params": {
            "index": "kubernetesqua-logs-*",
            "body": {
                "query": {
                    "bool": {
                        "must": [
                            {"match_phrase": {"tag": "kubernetes.var.log.containers.docgenius"}},
                            {"range": {"@timestamp": {"gte": start_date}}}
                        ]
                    }
                },
                "size": BATCH_SIZE,
                "sort": [{"@timestamp": "asc"}]
            }
        }
    }
    
    if search_after:
        query["params"]["body"]["search_after"] = search_after
    
    return query


async def _save_logs_to_file(logs: list, file_path: str) -> None:
    """
    Save logs to a file.

    Parameters
    ----------
    logs : list
        List of log entries to save
    file_path : str
        Path to the output file
    """
    with open(file_path, "w") as file:
        for log in logs:
            file.write(f"{log}\n")
    logging.info(f"Logs saved to: {file_path}")


async def fetch_kibana_logs() -> str:
    """
    Fetch logs from Kibana API.

    Returns
    -------
    str
        Path to the file containing the saved logs

    Notes
    -----
    Fetches logs in batches and saves them to a file in the LOG_DIR directory
    """
    all_logs = []
    start_date = await _get_start_date()
    last_sort_value = None
    log_file_path = os.path.join(LOG_DIR, "kibana_logs.txt")

    # Ensure log directory exists
    os.makedirs(LOG_DIR, exist_ok=True)
    
    logging.info(f"Fetching Kibana logs from {start_date}...")

    while True:
        query = _create_kibana_query(start_date, last_sort_value)
        response = requests.post(KIBANA_URL, headers=HEADERS, json=query)

        if response.status_code != 200:
            logging.error(f"Failed to fetch Kibana logs: {response.status_code} - {response.text}")
            break

        logs = response.json()
        hits = logs.get("rawResponse", {}).get("hits", {}).get("hits", [])
        
        if not hits:
            break

        all_logs.extend([hit["_source"] for hit in hits])
        last_sort_value = hits[-1]["sort"]

    await _save_logs_to_file(all_logs, log_file_path)
    logging.info(f"Total logs extracted: {len(all_logs)}")
    
    return log_file_path
