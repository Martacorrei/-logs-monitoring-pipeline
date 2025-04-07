import logging

import pandas as pd


# Configure logging
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def process_request_errors(df_req_logs: pd.DataFrame) -> pd.DataFrame:
    """
    Process request error logs.

    Parameters
    ----------
    df_req_logs : pd.DataFrame
        DataFrame containing request logs with columns:
        [timestamp, req_id, level, error_detail, error_message, status_code]

    Returns
    -------
    pd.DataFrame
        Processed error logs with columns:
        [timestamp, req_id, level, error_detail, status_code]

    Notes
    -----
    Only processes non-INFO level logs and removes duplicate request IDs
    """

    df_req_error_logs = df_req_logs[df_req_logs["level"] != "INFO"]
    
    if df_req_error_logs.empty:
        logging.info("No errors found in request logs")
        return pd.DataFrame()

    seen_ids = set()
    has_error_detail = "error_detail" in df_req_error_logs.columns
    has_error_message = "error_message" in df_req_error_logs.columns

    if not has_error_detail and not has_error_message:
        logging.warning("Neither 'error_detail' nor 'error_message' columns exist")
        return pd.DataFrame()
    
    data = []
    for _, row in df_req_error_logs.iterrows():
        if row['req_id'] not in seen_ids:
            seen_ids.add(row['req_id'])
            error_detail = (row.get("error_detail") if has_error_detail and pd.notna(row.get("error_detail")) 
                          else row.get("error_message") if has_error_message and pd.notna(row.get("error_message")) 
                          else None)
            
            status_code = df_req_logs.loc[df_req_logs['req_id'] == row['req_id'], 'status_code'].dropna().unique()
            status_code_str = status_code[0] if len(status_code) > 0 else ""
            
            data.append({
                "timestamp": row['timestamp'],
                "req_id": row['req_id'],
                "level": row['level'],
                "error_detail": error_detail,
                "status_code": status_code_str
            })

    if data:
        df_req_errors = pd.DataFrame(data)
        logging.info(f"Successfully processed error records: {len(df_req_errors)}")
        return df_req_errors

    logging.info("No errors found in request logs")
    return pd.DataFrame()


def process_request_login(df_req_logs: pd.DataFrame) -> pd.DataFrame:
    """
    Process login attempt logs from request logs.

    Parameters
    ----------
    df_req_logs : pd.DataFrame
        DataFrame containing request logs with columns:
        [timestamp, req_id, login, duration_s, status_code, error_message, 
         error_detail, ip]

    Returns
    -------
    pd.DataFrame
        Processed login logs with successful authentication attempts

    Notes
    -----
    Only processes complete login records with duration and status code
    """

    login_data = []
    
    try:
        for unique_req_id in df_req_logs['req_id'].unique():
            filtered_logs = df_req_logs[df_req_logs['req_id'] == unique_req_id]

            # Extract login information with column existence validation
            login = (filtered_logs['login'].dropna().iloc[0] 
                    if 'login' in filtered_logs.columns and not filtered_logs['login'].dropna().empty 
                    else None)
                
            duration = (filtered_logs['duration_s'].dropna().iloc[0] 
                       if 'duration_s' in filtered_logs.columns and not filtered_logs['duration_s'].dropna().empty 
                       else None)
                
            status_code = (filtered_logs['status_code'].dropna().iloc[0] 
                          if 'status_code' in filtered_logs.columns and not filtered_logs['status_code'].dropna().empty 
                          else None)

            if all([login, duration, status_code]):
                login_entry = {
                    "timestamp": filtered_logs['timestamp'].iloc[0],
                    "req_id": unique_req_id,
                    "login": login,
                    "duration": duration,
                    "status_code": status_code,
                    "error_message": filtered_logs.get('error_message', pd.Series()).dropna().iloc[0] if not filtered_logs.get('error_message', pd.Series()).dropna().empty else None,
                    "error_detail": filtered_logs.get('error_detail', pd.Series()).dropna().iloc[0] if not filtered_logs.get('error_detail', pd.Series()).dropna().empty else None,
                    "ip": filtered_logs['ip'].iloc[0]
                }
                login_data.append(login_entry)

        if not login_data:
            logging.info("No login logs found in request logs")
            return pd.DataFrame()
        
        df_req_login = pd.DataFrame(login_data)
        logging.info(f"Successfully processed login records: {len(df_req_login)}")
        return df_req_login
    
    except Exception as e:
        logging.error(f"Error processing login logs: {e}")
        raise e


def process_request_path_duration(df_req_logs: pd.DataFrame) -> pd.DataFrame:
    """
    Process path and duration information from request logs.

    Parameters
    ----------
    df_req_logs : pd.DataFrame
        DataFrame containing request logs with columns:
        [method, path_edited, duration_s, req_id, status_code, timestamp]

    Returns
    -------
    pd.DataFrame
        Processed path duration information with valid duration values

    Notes
    -----
    Converts duration to float and removes rows with missing duration
    """

    try:
        df_req_logs['duration_s'] = df_req_logs['duration_s'].astype(float)
        df_req_path_duration = df_req_logs[
            ['method', 'path_edited', 'duration_s', 'req_id', 'status_code', 'timestamp']
        ].dropna(subset=['duration_s'])

        if not df_req_path_duration.empty:
            logging.info(f"Successfully processed path duration records: {len(df_req_path_duration)}")
            return df_req_path_duration
        
        logging.info("No path duration data found in request logs")
        return pd.DataFrame()
    
    except Exception as e:
        logging.error(f"Error processing path duration logs: {e}")
        return pd.DataFrame()
