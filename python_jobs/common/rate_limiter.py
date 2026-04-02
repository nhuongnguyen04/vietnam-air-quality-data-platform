"""
Rate Limiter Module - Token Bucket implementation for API rate limiting.

This module provides a TokenBucketRateLimiter class that implements the token bucket
algorithm to control request rates to external APIs. It ensures compliance with API
rate limits while allowing small bursts.

OpenAQ API limits: 60 requests/minute, 2000 requests/hour
AQICN API limits: Varies by subscription

Author: Air Quality Data Platform
"""

import time
import threading
from collections import deque
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class TokenBucketRateLimiter:
    """
    Token Bucket rate limiter with proactive rate limiting.
    
    This implementation combines:
    - Token bucket algorithm for smooth rate control
    - Sliding window for precise per-minute tracking
    - Exponential backoff for handling 429 errors
    
    Usage:
        limiter = TokenBucketRateLimiter(rate_per_second=0.8, burst_size=4)
        await limiter.acquire()
        # make API call
    """
    
    def __init__(
        self,
        rate_per_second: float = 1.0,
        burst_size: int = 5,
        initial_delay: float = 1.0,
        max_delay: float = 180.0,
        backoff_factor: float = 2.0,
        jitter: float = 0.25,
        max_retries: int = 8,
        requests_per_minute: Optional[float] = None
    ):
        """
        Initialize the rate limiter.
        
        Args:
            rate_per_second: Maximum sustained rate of requests per second
            burst_size: Maximum number of requests that can be made in a burst
            initial_delay: Initial delay between retries (seconds)
            max_delay: Maximum delay between retries (seconds)
            backoff_factor: Multiplier for exponential backoff
            jitter: Random jitter factor (0-1) to add to delays
            max_retries: Maximum number of retries on rate limit errors
            requests_per_minute: Optional alternative rate limit (requests/minute)
        """
        self.rate_per_second = rate_per_second
        self.burst_size = burst_size
        
        # Token bucket state
        self._tokens = float(burst_size)
        self._last_update = time.time()
        self._lock = threading.Lock()
        
        # Sliding window for per-minute tracking
        self._requests_this_minute = 0
        self._minute_window = deque(maxlen=60)  # Track last 60 seconds
        self._minute_window_lock = threading.Lock()
        
        # Backoff configuration
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        self.max_retries = max_retries
        
        # If requests_per_minute is specified, use it instead
        if requests_per_minute is not None:
            self.rate_per_second = requests_per_minute / 60.0
        
        logger.info(
            f"RateLimiter initialized: rate={rate_per_second} req/s, "
            f"burst={burst_size}, max_delay={max_delay}s"
        )
    
    def _add_jitter(self, delay: float) -> float:
        """Add random jitter to delay."""
        import random
        jitter_amount = delay * self.jitter * random.uniform(-1, 1)
        return max(0, delay + jitter_amount)
    
    def _refill_tokens(self) -> None:
        """Refill tokens based on time elapsed since last update."""
        now = time.time()
        elapsed = now - self._last_update
        
        # Add tokens based on elapsed time
        new_tokens = elapsed * self.rate_per_second
        self._tokens = min(self.burst_size, self._tokens + new_tokens)
        self._last_update = now
    
    def _cleanup_minute_window(self) -> None:
        """Remove requests older than 60 seconds from the sliding window."""
        now = time.time()
        cutoff = now - 60.0
        
        while self._minute_window and self._minute_window[0] < cutoff:
            self._minute_window.popleft()
    
    def _can_make_request(self) -> bool:
        """
        Check if a request can be made without exceeding rate limits.
        
        Returns True if:
        - We have tokens available in the bucket
        - We haven't exceeded the per-minute limit
        """
        with self._lock:
            self._refill_tokens()
            has_tokens = self._tokens >= 1.0
        
        with self._minute_window_lock:
            self._cleanup_minute_window()
            minute_limit_ok = len(self._minute_window) < 60  # Max 60/min
        
        return has_tokens and minute_limit_ok
    
    def _wait_time(self) -> float:
        """
        Calculate how long to wait before next request.
        
        Returns:
            Wait time in seconds
        """
        with self._lock:
            self._refill_tokens()
            if self._tokens >= 1.0:
                token_wait = 0.0
            else:
                token_wait = (1.0 - self._tokens) / self.rate_per_second
        
        with self._minute_window_lock:
            self._cleanup_minute_window()
            if len(self._minute_window) >= 60:
                # Need to wait for oldest request to expire
                oldest = self._minute_window[0]
                minute_wait = max(0, oldest + 60.0 - time.time())
            else:
                minute_wait = 0.0
        
        return max(token_wait, minute_wait)
    
    def acquire(self) -> None:
        """
        Acquire permission to make a request.
        
        Blocks until a request can be made without exceeding rate limits.
        """
        while not self._can_make_request():
            wait_time = self._wait_time()
            logger.debug(f"Rate limit: waiting {wait_time:.2f}s")
            time.sleep(wait_time)
        
        # Consume a token
        with self._lock:
            self._tokens -= 1.0
        
        # Record request in sliding window
        with self._minute_window_lock:
            self._minute_window.append(time.time())
    
    async def acquire_async(self) -> None:
        """
        Async version of acquire for use with asyncio.
        """
        import asyncio
        while not self._can_make_request():
            wait_time = self._wait_time()
            logger.debug(f"Rate limit: waiting {wait_time:.2f}s")
            await asyncio.sleep(wait_time)
        
        with self._lock:
            self._tokens -= 1.0
        
        with self._minute_window_lock:
            self._minute_window.append(time.time())
    
    def record_response(self, status_code: int, retry_count: int = 0) -> bool:
        """
        Record API response and determine if retry is needed.
        
        Args:
            status_code: HTTP status code from response
            retry_count: Current retry count
            
        Returns:
            True if request was successful and no retry needed
            False if should retry (rate limited)
        """
        if status_code == 429:
            # Rate limited - calculate backoff
            delay = min(
                self.initial_delay * (self.backoff_factor ** retry_count),
                self.max_delay
            )
            delay = self._add_jitter(delay)
            logger.warning(
                f"Rate limit hit (429). Retrying in {delay:.2f}s "
                f"(retry {retry_count + 1}/{self.max_retries})"
            )
            time.sleep(delay)
            return False
        
        return True
    
    def get_stats(self) -> dict:
        """Get current rate limiter statistics."""
        with self._lock:
            tokens = self._tokens
        with self._minute_window_lock:
            self._cleanup_minute_window()
            requests_this_min = len(self._minute_window)
        
        return {
            "available_tokens": tokens,
            "burst_size": self.burst_size,
            "requests_this_minute": requests_this_min,
            "rate_per_second": self.rate_per_second
        }


class AdaptiveRateLimiter(TokenBucketRateLimiter):
    """
    Adaptive rate limiter that adjusts based on 429 responses.
    
    Starts with conservative rate and increases if no rate limits encountered.
    Decreases rate if rate limits are hit.
    """
    
    def __init__(
        self,
        initial_rate: float = 0.5,  # Start with 30/min
        min_rate: float = 0.1,     # Min 6/min
        max_rate: float = 1.0,      # Max 60/min
        success_threshold: int = 10,
        **kwargs
    ):
        super().__init__(rate_per_second=initial_rate, **kwargs)
        
        self.initial_rate = initial_rate
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.success_threshold = success_threshold
        
        self._consecutive_successes = 0
        self._consecutive_failures = 0
    
    def record_response(self, status_code: int, retry_count: int = 0) -> bool:
        """Record response and adjust rate adaptively."""
        success = super().record_response(status_code, retry_count)
        
        if success and status_code != 429:
            self._consecutive_successes += 1
            self._consecutive_failures = 0
            
            # Increase rate if many consecutive successes
            if self._consecutive_successes >= self.success_threshold:
                new_rate = min(self.rate_per_second * 1.2, self.max_rate)
                if new_rate != self.rate_per_second:
                    logger.info(f"Increasing rate to {new_rate:.2f} req/s")
                    self.rate_per_second = new_rate
                    self._consecutive_successes = 0
        else:
            self._consecutive_failures += 1
            self._consecutive_successes = 0
            
            # Decrease rate if rate limited
            if status_code == 429:
                new_rate = max(self.rate_per_second * 0.5, self.min_rate)
                if new_rate != self.rate_per_second:
                    logger.warning(f"Decreasing rate to {new_rate:.2f} req/s")
                    self.rate_per_second = new_rate
        
        return success


# Factory functions for common use cases
def create_openaq_limiter() -> TokenBucketRateLimiter:
    """
    Create a rate limiter configured for OpenAQ API.
    
    OpenAQ limits: 60 requests/minute, 2000 requests/hour
    Using 48/min (0.8/s) for safety margin
    """
    return TokenBucketRateLimiter(
        rate_per_second=0.8,      # ~48/min
        burst_size=4,             # Small burst allowed
        max_delay=120.0,          # Longer backoff for rate limits
        requests_per_minute=48.0
    )


def create_aqicn_limiter() -> TokenBucketRateLimiter:
    """
    Create a rate limiter configured for AQICN API.

    Using conservative defaults - adjust based on API token tier.
    """
    return TokenBucketRateLimiter(
        rate_per_second=1.0,     # 60/min
        burst_size=5,
        max_delay=60.0
    )


def create_openweather_limiter() -> TokenBucketRateLimiter:
    """
    Create a rate limiter configured for OpenWeather Air Pollution API.

    OpenWeather free tier: 60 requests/minute, 1M calls/month.
    Using ~50/min (0.8/s) for safety margin.
    """
    return TokenBucketRateLimiter(
        rate_per_second=0.8,      # ~48/min safe
        burst_size=4,
        max_delay=300.0,          # 5min max backoff (D-31)
        backoff_factor=2.0,
    )


def create_sensorscm_limiter() -> TokenBucketRateLimiter:
    """
    Create a rate limiter configured for Sensors.Community API.

    Sensors.Community has no authentication and no published rate limits.
    Using 1.0 req/s (60/min) as a courtesy maximum to avoid overwhelming
    the community API infrastructure.

    Reference: 01-RESEARCH.md § Sensors.Community — no auth, rate_per_second=1.0
    """
    return TokenBucketRateLimiter(
        rate_per_second=1.0,    # ~60/min courtesy limit
        burst_size=5,
        max_delay=300.0,        # 5min max backoff (D-31)
        backoff_factor=2.0,    # exponential backoff (D-31)
    )

