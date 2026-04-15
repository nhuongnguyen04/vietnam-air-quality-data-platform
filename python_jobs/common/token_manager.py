"""
Token Manager - Manager for multi-token rotation and rate limiting.

This module provides a TokenManager class that rotates through multiple API keys
and manages their individual rate limits to maximize total throughput.

Author: Air Quality Data Platform
"""

import logging
import threading
import time
from typing import List, Tuple, Optional
from .rate_limiter import TokenBucketRateLimiter

logger = logging.getLogger(__name__)

class TokenManager:
    """
    Manages a pool of API tokens, each with its own individual rate limiter.
    
    This allows the system to multiply its total throughput by the number of 
    available tokens while remaining within the limits of each individual key.
    """
    
    def __init__(
        self, 
        tokens: List[str], 
        requests_per_minute: float = 54.0,
        burst_size: int = 4
    ):
        """
        Initialize the TokenManager.
        
        Args:
            tokens: List of API tokens to use
            requests_per_minute: Max RPM per token (defaults to safe 54)
            burst_size: Max burst per token
        """
        if not tokens:
            raise ValueError("TokenManager requires at least one token")
            
        self.tokens = tokens
        self.limiters = [
            TokenBucketRateLimiter(
                requests_per_minute=requests_per_minute,
                burst_size=burst_size
            ) for _ in tokens
        ]
        self.active_mask = [True] * len(tokens)  # Track which tokens are active
        self.fail_counts = [0] * len(tokens)      # Track consecutive failures
        
        # Round-robin selection state
        self._current_index = 0
        self._lock = threading.Lock()
        
        logger.info(f"TokenManager initialized with {len(tokens)} tokens. Total possible throughput: {len(tokens) * requests_per_minute} RPM.")

    def get_token_and_limiter(self) -> Tuple[str, TokenBucketRateLimiter]:
        """
        Selects the next token and its associated rate limiter using round-robin.
        
        Returns:
            A tuple of (token, limiter)
        """
        with self._lock:
            # Strategy: find the next available and active token
            search_count = 0
            while search_count < len(self.tokens):
                index = self._current_index
                self._current_index = (self._current_index + 1) % len(self.tokens)
                search_count += 1
                
                if self.active_mask[index]:
                    return self.tokens[index], self.limiters[index], index
            
            # If no active tokens, fallback to the first one (safety)
            return self.tokens[0], self.limiters[0], 0

    def mark_failed(self, index: int, status_code: int):
        """Mark a token as failed. If 401, disable it permanently for this run."""
        with self._lock:
            if status_code == 401:
                logger.error(f"Token at index {index} is INVALID (401). Disabling it.")
                self.active_mask[index] = False
            else:
                self.fail_counts[index] += 1
                if self.fail_counts[index] > 5:
                    logger.warning(f"Token at index {index} failed 5 times. Temporarily disabling.")
                    self.active_mask[index] = False

    def get_total_rpm(self) -> float:
        """Return the total combined RPM support."""
        active_count = sum(1 for x in self.active_mask if x)
        return active_count * 54.0

    def get_token_count(self) -> int:
        """Return number of tokens."""
        return len(self.tokens)
