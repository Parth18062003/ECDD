"""
ECDD Utilities - Type Coercion and Helpers

Provides robust type conversion to handle LLM output inconsistencies.
All functions handle None, invalid types, and edge cases gracefully.
"""

import json
import logging
from datetime import datetime, timezone, date
from typing import Any, Optional, Dict, List, Union

logger = logging.getLogger(__name__)


# =============================================================================
# DATE/TIME COERCION
# =============================================================================

def ensure_string_timestamp(value: Any) -> str:
    """
    Convert any value to an ISO timestamp string.
    
    Handles:
    - datetime objects
    - date objects
    - strings (validates format)
    - None (returns current time)
    - Invalid types (returns current time)
    """
    if value is None:
        return datetime.now(timezone.utc).isoformat()
    
    if isinstance(value, datetime):
        return value.isoformat()
    
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc).isoformat()
    
    if isinstance(value, str):
        # Already a string - validate it's a reasonable format
        if value.strip():
            return value
        return datetime.now(timezone.utc).isoformat()
    
    # Fallback for any other type
    try:
        return str(value)
    except:
        return datetime.now(timezone.utc).isoformat()


def format_display_date(value: Any, fmt: str = "%Y-%m-%d") -> str:
    """
    Format any timestamp value for display.
    
    Args:
        value: Timestamp value (datetime, date, or string)
        fmt: strftime format string
        
    Returns:
        Formatted string, or first 10 chars for ISO strings
    """
    if value is None:
        return "N/A"
    
    if isinstance(value, datetime):
        return value.strftime(fmt)
    
    if isinstance(value, date):
        return value.strftime(fmt)
    
    if isinstance(value, str):
        # Try to parse and format
        if len(value) >= 10:
            return value[:10]  # Return just the date portion
        return value
    
    return str(value)[:10] if str(value) else "N/A"


def format_display_datetime(value: Any, fmt: str = "%Y-%m-%d %H:%M") -> str:
    """
    Format any timestamp value for display with time.
    """
    if value is None:
        return "N/A"
    
    if isinstance(value, datetime):
        return value.strftime(fmt)
    
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    
    if isinstance(value, str):
        if len(value) >= 16:
            return value[:16].replace("T", " ")
        return value
    
    return str(value)


# =============================================================================
# TYPE COERCION UTILITIES
# =============================================================================

def ensure_string(value: Any, default: str = "") -> str:
    """Safely convert any value to string."""
    if value is None:
        return default
    if isinstance(value, str):
        return value
    try:
        return str(value)
    except:
        return default


def ensure_float(value: Any, default: float = 0.0) -> float:
    """Safely convert any value to float."""
    if value is None:
        return default
    if isinstance(value, float):
        return value
    if isinstance(value, int):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return default
    return default


def ensure_int(value: Any, default: int = 0) -> int:
    """Safely convert any value to int."""
    if value is None:
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value))
        except ValueError:
            return default
    return default


def ensure_bool(value: Any, default: bool = False) -> bool:
    """Safely convert any value to bool."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ('true', 'yes', '1', 'on')
    if isinstance(value, (int, float)):
        return bool(value)
    return default


def ensure_list(value: Any, default: List = None) -> List:
    """Safely convert any value to list."""
    if default is None:
        default = []
    if value is None:
        return default
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, dict):
        return [value]
    return [value]


def ensure_dict(value: Any, default: Dict = None) -> Dict:
    """Safely convert any value to dict."""
    if default is None:
        default = {}
    if value is None:
        return default
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass
    return default


# =============================================================================
# JSON UTILITIES
# =============================================================================

def safe_json_dumps(obj: Any, default: str = "{}") -> str:
    """Safely convert any object to JSON string."""
    try:
        return json.dumps(obj, default=str)
    except Exception as e:
        logger.warning(f"JSON serialization failed: {e}")
        return default


def safe_json_loads(s: str, default: Any = None) -> Any:
    """Safely parse JSON string."""
    if default is None:
        default = {}
    if not s or not isinstance(s, str):
        return default
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return default


# =============================================================================
# MODEL COERCION
# =============================================================================

def coerce_to_risk_level(value: Any) -> str:
    """Convert value to valid risk level string."""
    valid_levels = {'low', 'medium', 'high', 'critical'}
    
    if value is None:
        return 'medium'
    
    str_value = str(value).lower().strip()
    
    if str_value in valid_levels:
        return str_value
    
    # Handle variations
    if 'low' in str_value:
        return 'low'
    if 'high' in str_value or 'elevated' in str_value:
        return 'high'
    if 'critical' in str_value or 'severe' in str_value:
        return 'critical'
    
    return 'medium'


def coerce_to_document_priority(value: Any) -> str:
    """Convert value to valid document priority string."""
    valid_priorities = {'required', 'recommended', 'optional'}
    
    if value is None:
        return 'required'
    
    str_value = str(value).lower().strip()
    
    if str_value in valid_priorities:
        return str_value
    
    return 'required'


def coerce_to_session_status(value: Any) -> str:
    """Convert value to valid session status string."""
    valid_statuses = {
        'pending', 'questionnaire_generated', 'in_progress', 
        'responses_submitted', 'reports_generated', 'pending_review',
        'approved', 'escalated', 'rejected', 'error'
    }
    
    if value is None:
        return 'pending'
    
    str_value = str(value).lower().strip().replace(' ', '_')
    
    if str_value in valid_statuses:
        return str_value
    
    return 'pending'
