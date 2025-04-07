import logging
from typing import Optional, Dict

from src.mail import Mail


# Configure logging
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def _format_email_body(result_tables: Optional[Dict[str, int]]) -> str:
    """
    Format the email body based on log processing results.

    Parameters
    ----------
    result_tables : Optional[Dict[str, int]]
        Dictionary containing table names and their row counts
        None if no logs were processed

    Returns
    -------
    str
        Formatted HTML email body
    """
    body_lines = [""]

    if result_tables is None:
        body_lines.append("⚠️ <b>No logs were processed.</b>\n")
        body_lines.append("Please check the airflow logs and try again.\n")
    else:
        body_lines.append("✅ <b>Logs were processed and refreshed successfully:</b>\n")
        for table, row_count in result_tables.items():
            body_lines.append(f" - {table}: {row_count} rows\n")

    return "".join(body_lines)


def send_email_docgenius(result_tables: Optional[Dict[str, int]]) -> None:
    """
    Send email notification about DocGenius log processing results.

    Parameters
    ----------
    result_tables : Optional[Dict[str, int]]
        Dictionary containing table names and their row counts
        None if no logs were processed

    Notes
    -----
    Uses the Mail class to send HTML-formatted emails with processing results
    """
    try:
        logging.info("Sending email with DocGenius logs processing results...")
        
        body = _format_email_body(result_tables)
        mail_instance = Mail()
        mail_instance.send_email(
            subject="DocGenius Logs Update",
            body=body
        )
        
        logging.info("Email sent successfully")
    
    except Exception as e:
        logging.error(f"Failed to send email: {e}")
