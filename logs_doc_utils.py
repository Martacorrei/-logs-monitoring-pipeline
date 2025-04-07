import logging
import re
from typing import Dict

import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def process_doc_msg(row: Dict) -> Dict:
    """
    Process document message content and extract relevant information.

    Parameters
    ----------
    row : Dict
        Dictionary containing log row data with 'msg' field

    Returns
    -------
    Dict
        Processed row with extracted information

    Notes
    -----
    Extracts the following information from messages:
    - Number of invoices created
    - Process invoice finish time
    - Input/Output tokens
    """
    if row['msg'] is not None:
        if row['msg'].startswith('msg: Number of invoices created - '):
            match = re.search(r'Number of invoices created - (\d+)', row['msg'])
            if match:
                row['number_of_invoices_created'] = int(match.group(1))
            row['msg'] = None
        elif row['msg'].startswith('msg: PROCESS invoice finished'):
            match = re.search(r'PROCESS invoice finished in (\d+\.\d+) seconds', row['msg'])
            if match:
                row['process_invoice_finished_time'] = float(match.group(1))
                row['msg'] = 'PROCESS invoice finished'
            else:
                row['msg'] = None
        elif 'Input tokens' in row['msg'] and 'Output tokens' in row['msg']:
            match = re.search(r'Input tokens (\d+)', row['msg'])
            if match:
                row['input_tokens'] = int(match.group(1))
            match = re.search(r'Output tokens (\d+)', row['msg'])
            if match:
                row['output_tokens'] = int(match.group(1))
            row['msg'] = None
    return row


def process_document_errors(df_doc_logs: pd.DataFrame) -> pd.DataFrame:
    """
    Process document error logs.

    Parameters
    ----------
    df_doc_logs : pd.DataFrame
        DataFrame containing document logs with columns:
        [timestamp, doc_id, level, msg, error_message, error_detail]

    Returns
    -------
    pd.DataFrame
        Processed error logs with columns:
        [timestamp, doc_id, level, msg]

    Notes
    -----
    Only processes non-INFO level logs
    """
    doc_errors_data = []
    for _, row in df_doc_logs[df_doc_logs["level"] != "INFO"].iterrows():
        msg_value = row['msg']
        if pd.notna(row.get('error_message')):
            msg_value = row['error_message']
        elif pd.notna(row.get('error_detail')):
            msg_value = row['error_detail']
        
        doc_errors_data.append({
            "timestamp": row['timestamp'],
            "doc_id": row.get('doc_id', ''),
            "level": row['level'],
            "msg": msg_value
        })

    if not doc_errors_data:
        logging.info("No document errors found")
        return pd.DataFrame()

    df_doc_errors = pd.DataFrame(doc_errors_data)
    logging.info(f"Successfully processed document error records: {len(df_doc_errors)}")
    return df_doc_errors


def process_document_time(df_doc_logs: pd.DataFrame) -> pd.DataFrame:
    """
    Process document processing times.

    Parameters
    ----------
    df_doc_logs : pd.DataFrame
        DataFrame containing document logs with columns:
        [doc_id, process_invoice_finished_time, number_of_invoices_created, timestamp]

    Returns
    -------
    pd.DataFrame
        Processed document times with processing duration and invoice counts

    Notes
    -----
    Requires 'process_invoice_finished_time' column to be present
    """

    if 'process_invoice_finished_time' not in df_doc_logs.columns:
        logging.warning("Column 'process_invoice_finished_time' not found in DataFrame")
        return pd.DataFrame()

    filtered_logs = df_doc_logs[df_doc_logs['process_invoice_finished_time'].notna()]
    
    docs_time_data = []
    for _, row in filtered_logs.iterrows():
        doc_id = row['doc_id']
        
        number_of_invoices = df_doc_logs.loc[
            df_doc_logs['doc_id'] == doc_id, 
            'number_of_invoices_created'
        ].dropna().unique()
        
        number_of_invoices_created = number_of_invoices[0] if len(number_of_invoices) > 0 else None
        
        docs_time_data.append({
            'doc_id': doc_id,
            'process_invoice_finished_time': row['process_invoice_finished_time'],
            'number_of_invoices_created': number_of_invoices_created,
            'timestamp': row['timestamp']
        })
    
    if not docs_time_data:
        logging.info("No document processing times found")
        return pd.DataFrame()
    
    df_doc_time = pd.DataFrame(docs_time_data)
    logging.info(f"Successfully processed document time records: {len(df_doc_time)}")
    return df_doc_time


def process_document_duration(df_doc_logs):
    """
    Process document logs duration.

    Parameters
    ----------
    df_doc_logs : pd.DataFrame
        DataFrame containing document logs with columns:
        [timestamp, msg, doc_id, duration]

    Returns
    -------
    pd.DataFrame
        Processed duration logs with timing information

    Notes
    -----
    Only processes logs with valid duration values
    """

    doc_logs_duration_data = []
    for unique_msg in df_doc_logs['msg'].unique():
        filtered_logs = df_doc_logs[df_doc_logs['msg'] == unique_msg]
        
        for _, row in filtered_logs.iterrows():
            if pd.notna(row.get('duration')):
                doc_logs_duration_data.append({
                    "timestamp": row['timestamp'],
                    "msg": row['msg'],
                    "doc_id": row.get('doc_id', ''),
                    "duration": row['duration']
                })
    
    if not doc_logs_duration_data:
        logging.info("No document duration logs found")
        return pd.DataFrame()
    
    df_doc_logs_duration = pd.DataFrame(doc_logs_duration_data)
    logging.info(f"Successfully processed document duration records: {len(df_doc_logs_duration)}")
    return df_doc_logs_duration
