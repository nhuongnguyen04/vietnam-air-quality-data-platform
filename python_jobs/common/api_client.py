"""
API Client Module - HTTP client with retry logic for external APIs.

This module provides a generic APIClient class with:
- Automatic retry with exponential backoff
- Rate limiting support
- Request/response logging
- Error handling

Author: Air Quality Data Platform
"""

import time
import json
import logging
from typing import Optional, Dict, Any, Callable
from urllib.parse import urljoin, urlencode
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class APIClient:
    """
    Generic HTTP API client with retry logic and rate limiting.
    
    Features:
    - Configurable retry strategy
    - Rate limiting integration
    - Request/response logging
    - JSON parsing
    
    Usage:
        client = APIClient(base_url="https://api.openaq.org/", token="your_token")
        response = client.get("/v3/parameters", params={"country": "VN"})
    """
    
    def __init__(
        self,
        base_url: str,
        token: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 5,
        backoff_factor: float = 2.0,  # base=2 exponential backoff (D-31)
        rate_limiter: Optional[Callable] = None,
        headers: Optional[Dict[str, str]] = None,
        auth_header_name: Optional[str] = "Authorization",
        auth_header_format: str = "Token {}"
    ):
        """
        Initialize the API client.
        
        Args:
            base_url: Base URL for all API requests
            token: API token for authentication
            timeout: Request timeout in seconds
            max_retries: Maximum number of retries on failure
            backoff_factor: Exponential backoff factor
            rate_limiter: Optional rate limiter (callable that blocks until allowed)
            headers: Additional headers to include in requests
            auth_header_name: Name of the authorization header
            auth_header_format: Format string for the authorization header value
        """
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.timeout = timeout
        self.rate_limiter = rate_limiter
        
        # Setup session with retry strategy
        self.session = requests.Session()
        
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"],
            raise_on_status=False
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Default headers
        self.default_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        if token and auth_header_name:
            self.default_headers[auth_header_name] = auth_header_format.format(token)
        
        if headers:
            self.default_headers.update(headers)
        
        logger.info(f"APIClient initialized: base_url={base_url}")
    
    def _build_url(self, endpoint: str, params: Optional[Dict] = None) -> str:
        """Build full URL with query parameters."""
        url = urljoin(self.base_url + "/", endpoint.lstrip('/'))
        
        if params:
            # Filter out None values
            filtered_params = {k: v for k, v in params.items() if v is not None}
            if filtered_params:
                url = f"{url}?{urlencode(filtered_params)}"
        
        return url
    
    def _log_request(self, method: str, url: str, params: Optional[Dict] = None) -> None:
        """Log outgoing request."""
        logger.debug(f"REQUEST: {method} {url}")
        if params:
            logger.debug(f"PARAMS: {json.dumps(params, default=str)}")
    
    def _log_response(self, status_code: int, url: str, elapsed: float) -> None:
        """Log incoming response."""
        logger.debug(f"RESPONSE: {status_code} {url} ({elapsed:.2f}s)")
    
    def request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        skip_rate_limit: bool = False
    ) -> Dict[str, Any]:
        """
        Make an HTTP request with retry logic.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            params: Query parameters
            data: Form data
            json_data: JSON body
            headers: Additional headers
            skip_rate_limit: Skip rate limiting for this request
            
        Returns:
            JSON response as dictionary
            
        Raises:
            requests.HTTPError: On HTTP error status codes
            requests.RequestException: On connection errors
        """
        # Apply rate limiting
        if self.rate_limiter and not skip_rate_limit:
            self.rate_limiter.acquire()
        
        url = self._build_url(endpoint, params)
        
        # Merge headers
        request_headers = self.default_headers.copy()
        if headers:
            request_headers.update(headers)
        
        self._log_request(method, url, params)
        
        start_time = time.time()
        
        try:
            response = self.session.request(
                method=method,
                url=url,
                data=data,
                json=json_data,
                headers=request_headers,
                timeout=self.timeout
            )
            
            elapsed = time.time() - start_time
            self._log_response(response.status_code, url, elapsed)
            
            # Handle different status codes
            if response.status_code == 429:
                # Rate limited - let retry strategy handle it
                logger.warning(f"Rate limited (429) for {url}")
                response.raise_for_status()
            
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            # Enhanced error logging with response body (important for 400 Bad Requests)
            try:
                error_detail = response.text if 'response' in locals() else "Unknown"
                logger.error(f"HTTP Error: {e} | Response Body: {error_detail}")
            except:
                logger.error(f"HTTP Error: {e}")
            raise
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection Error: {e} for {url}")
            raise
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout: {e} for {url}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"JSON Decode Error: {e} for {url}")
            raise
    
    def get(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        skip_rate_limit: bool = False
    ) -> Dict[str, Any]:
        """Make a GET request."""
        return self.request("GET", endpoint, params=params, skip_rate_limit=skip_rate_limit)
    
    def post(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
        skip_rate_limit: bool = False
    ) -> Dict[str, Any]:
        """Make a POST request."""
        return self.request(
            "POST", endpoint, params=params, json_data=json_data, skip_rate_limit=skip_rate_limit
        )
    
    def put(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
        skip_rate_limit: bool = False
    ) -> Dict[str, Any]:
        """Make a PUT request."""
        return self.request(
            "PUT", endpoint, params=params, json_data=json_data, skip_rate_limit=skip_rate_limit
        )
    
    def delete(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        skip_rate_limit: bool = False
    ) -> Dict[str, Any]:
        """Make a DELETE request."""
        return self.request("DELETE", endpoint, params=params, skip_rate_limit=skip_rate_limit)
    
    def close(self) -> None:
        """Close the session."""
        self.session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


class PaginatedAPIClient(APIClient):
    """
    Extended API client with pagination support.
    
    Handles paginated responses automatically and provides iterators.
    """
    
    def __init__(
        self,
        base_url: str,
        token: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 5,
        rate_limiter: Optional[Callable] = None,
        page_param: str = "page",
        limit_param: str = "limit",
        max_pages: int = 100,
        headers: Optional[Dict[str, str]] = None,
        auth_header_name: Optional[str] = "Authorization",
        auth_header_format: str = "Token {}"
    ):
        """
        Initialize the paginated API client.
        
        Args:
            base_url: Base URL for API
            token: API token
            timeout: Request timeout
            max_retries: Max retries
            rate_limiter: Rate limiter
            page_param: Name of page parameter in API
            limit_param: Name of limit parameter in API
            max_pages: Maximum pages to fetch (safety limit)
            headers: Additional headers
            auth_header_name: Auth header name
            auth_header_format: Auth header format string
        """
        super().__init__(
            base_url=base_url,
            token=token,
            timeout=timeout,
            max_retries=max_retries,
            backoff_factor=1.5,
            rate_limiter=rate_limiter,
            headers=headers,
            auth_header_name=auth_header_name,
            auth_header_format=auth_header_format
        )
        
        self.page_param = page_param
        self.limit_param = limit_param
        self.max_pages = max_pages
    
    def fetch_all(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        limit: int = 1000,
        max_items: Optional[int] = None
    ) -> list:
        """
        Fetch all pages of a paginated endpoint.
        
        Args:
            endpoint: API endpoint
            params: Base query parameters
            limit: Number of items per page
            max_items: Maximum total items to fetch
            
        Returns:
            List of all items from all pages
        """
        params = params or {}
        params[self.limit_param] = limit
        
        all_results = []
        page = 1
        
        while page <= self.max_pages:
            params[self.page_param] = page
            
            logger.info(f"Fetching page {page} from {endpoint}")
            
            try:
                response = self.get(endpoint, params=params)
            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                break
            
            # Extract results based on response structure
            results = response.get("results", [])
            
            if not results:
                logger.info(f"No more results at page {page}")
                break
            
            all_results.extend(results)
            
            # Check if we've hit max items
            if max_items and len(all_results) >= max_items:
                all_results = all_results[:max_items]
                logger.info(f"Reached max_items limit: {max_items}")
                break
            
            # Check pagination info
            meta = response.get("meta", {})
            found = meta.get("found", 0)
            
            if found > 0 and len(all_results) >= found:
                logger.info(f"Fetched all {found} items")
                break
            
            page += 1
        
        logger.info(f"Fetched {len(all_results)} total items from {page - 1} pages")
        return all_results
    
    def fetch_all_with_generator(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        limit: int = 1000
    ):
        """
        Generator that yields pages as they are fetched.
        
        Use this for memory-efficient processing of large datasets.
        
        Args:
            endpoint: API endpoint
            params: Base query parameters
            limit: Number of items per page
            
        Yields:
            List of items for each page
        """
        params = params or {}
        params[self.limit_param] = limit
        
        page = 1
        
        while page <= self.max_pages:
            params[self.page_param] = page
            
            logger.debug(f"Fetching page {page} from {endpoint}")
            
            try:
                response = self.get(endpoint, params=params)
            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                break
            
            results = response.get("results", [])
            
            if not results:
                break
            
            yield results
            
            # Check if we've fetched everything
            meta = response.get("meta", {})
            found = meta.get("found", 0)
            
            if found > 0 and page * limit >= found:
                break
            
            page += 1




