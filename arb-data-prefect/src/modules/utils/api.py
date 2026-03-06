"""
Shared API utilities for HTTP requests and text normalization.

Usage:
    from modules.utils import api_get, normalize_title
    
    data, error = api_get("https://api.example.com/endpoint", params={"key": "value"})
    clean_title = normalize_title("  Some Event Title!!! ")
"""

import re
from typing import Any, Dict, Optional, Tuple

import requests


REQUEST_TIMEOUT = 30


def api_get(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = REQUEST_TIMEOUT,
) -> Tuple[Any, Optional[str]]:
    """
    Perform HTTP GET request with error handling.
    
    Args:
        url: Full URL to request
        params: Query parameters
        headers: Request headers
        timeout: Request timeout in seconds
        
    Returns:
        Tuple of (response_data, error_message)
        - response_data: Parsed JSON response (None on error)
        - error_message: Error description (None on success)
    """
    try:
        response = requests.get(
            url=url,
            params=params,
            headers=headers,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json(), None
    except requests.exceptions.Timeout:
        return None, f"Request timed out after {timeout}s"
    except requests.exceptions.ConnectionError:
        return None, "Connection error - check your internet connection"
    except requests.exceptions.HTTPError as exc:
        return None, f"HTTP error: {exc.response.status_code}"
    except requests.exceptions.RequestException as exc:
        return None, f"Request failed: {str(exc)}"
    except ValueError:
        return None, "Invalid JSON response"


def normalize_title(title: str) -> str:
    """
    Normalize event title for consistent matching.
    
    Operations:
    - Strip leading/trailing whitespace
    - Collapse multiple spaces to single space
    - Convert to lowercase for comparison
    - Remove excessive punctuation
    
    Args:
        title: Raw event title
        
    Returns:
        Cleaned, normalized title string
    """
    if not title:
        return ""
    
    # Strip whitespace
    cleaned = title.strip()
    
    # Collapse multiple whitespace to single space
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    # Remove leading/trailing punctuation that doesn't add meaning
    cleaned = re.sub(r'^[^\w\s]+|[^\w\s]+$', '', cleaned)
    
    return cleaned

