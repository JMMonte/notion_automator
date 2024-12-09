import logging
import time
from typing import Any, Callable, Tuple
from datetime import datetime
import pandas as pd
import re

logger = logging.getLogger(__name__)

def handle_api_call(notion_client, rate_limit_delay: float, max_retries: int, retry_delay: float, api_func: Callable, *args, **kwargs) -> Any:
    """Handle API calls with retries and rate limiting"""
    for attempt in range(max_retries):
        try:
            result = api_func(*args, **kwargs)
            time.sleep(rate_limit_delay)
            return result
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            logger.warning(f"API call failed, retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

def safe_format_date(date) -> str:
    """Safely format a date value to YYYY-MM-DD format"""
    try:
        if pd.isna(date):
            return None
        # Convert to datetime if not already
        if not isinstance(date, pd.Timestamp):
            date = pd.to_datetime(date)
        # Format to YYYY-MM-DD date string
        formatted_date = date.strftime("%Y-%m-%d")
        logger.debug(f"Successfully formatted date {date} to YYYY-MM-DD format: {formatted_date}")
        return formatted_date
    except Exception as e:
        logger.error(f"Failed to format date {date} (type: {type(date)}): {str(e)}")
        return None

def create_date_range(row: pd.Series, start_col: str, end_col: str, date_type: str = "") -> dict:
    """Create a date range dictionary from start and end dates"""
    start_date = None
    end_date = None
    
    # Convert start date
    if pd.notna(row.get(start_col)):
        try:
            start_date = pd.to_datetime(row[start_col])
            start_str = start_date.strftime("%Y-%m-%d")
        except Exception as e:
            logger.warning(f"Failed to parse start date {row[start_col]}: {e}")
            start_date = None
            start_str = None
    
    # Convert end date
    if pd.notna(row.get(end_col)):
        try:
            end_date = pd.to_datetime(row[end_col])
            end_str = end_date.strftime("%Y-%m-%d")
        except Exception as e:
            logger.warning(f"Failed to parse end date {row[end_col]}: {e}")
            end_date = None
            end_str = None
    
    # Handle invalid or missing dates
    if not start_str and not end_str:
        return None
    
    # If we only have one date, use it for both start and end
    if start_str and not end_str:
        return {"start": start_str, "end": start_str}
    if end_str and not start_str:
        return {"start": end_str, "end": end_str}
    
    # Validate that start is before end
    if start_date and end_date and start_date > end_date:
        # Swap dates if start is after end
        return {"start": end_str, "end": start_str}
    
    return {"start": start_str, "end": end_str}

def process_edt_code(edt_code: str) -> Tuple[str, str]:
    """
    Process EDT code to handle milestones and subtasks.
    Returns a tuple of (phase_code, edt_code).
    
    Examples:
    - PR.0001.1.4 -> (PR.0001.1, PR.0001.1.4)
    - PR.0001.1.4.M -> (PR.0001.1, PR.0001.1.4.M)
    - PR.0001.4.1.1 -> (PR.0001.4, PR.0001.4.1.1)
    """
    if not edt_code:
        return "", ""
        
    # Remove any whitespace
    edt_code = edt_code.strip()
    
    # Split the code into parts
    parts = edt_code.split('.')
    
    # Handle milestone case (ends with .M)
    if parts[-1] == 'M':
        # Use parent phase (e.g., PR.0001.1 for PR.0001.1.4.M)
        phase_code = '.'.join(parts[:-2])
        return phase_code, edt_code
        
    # Handle regular tasks and subtasks
    # For both PR.0001.1.4 and PR.0001.4.1.1, we want the phase to be the first three parts
    if len(parts) >= 3:
        phase_code = '.'.join(parts[:3])
        return phase_code, edt_code
        
    return edt_code, edt_code
