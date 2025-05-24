"""Utility functions for rate limiting, logging, and retries."""
import logging
import time
import random
from functools import wraps
from typing import Any, Callable, Optional, TypeVar, cast
from urllib.parse import urlparse, urlunparse
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Type variable for generic decorator
F = TypeVar('F', bound=Callable[..., Any])

# User agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
]


class RateLimiter:
    """Simple rate limiter with configurable delay."""
    
    def __init__(self, min_delay: float = 2.0, max_delay: float = 4.0):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.last_request = 0.0
    
    def wait(self) -> None:
        """Wait if necessary to respect rate limit."""
        now = time.time()
        elapsed = now - self.last_request
        delay = random.uniform(self.min_delay, self.max_delay)
        
        if elapsed < delay:
            sleep_time = delay - elapsed
            logging.debug(f"Rate limiting: sleeping for {sleep_time:.2f}s")
            time.sleep(sleep_time)
        
        self.last_request = time.time()


def retry_on_exception(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,)
) -> Callable[[F], F]:
    """Decorator to retry function on exception with exponential backoff."""
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            attempt = 1
            current_delay = delay
            
            while attempt <= max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        logging.error(f"{func.__name__} failed after {max_attempts} attempts: {e}")
                        raise
                    
                    logging.warning(
                        f"{func.__name__} attempt {attempt} failed: {e}. "
                        f"Retrying in {current_delay:.2f}s..."
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff
                    attempt += 1
            
            return None  # This should never be reached
        
        return cast(F, wrapper)
    return decorator


def get_random_user_agent() -> str:
    """Get a random user agent string."""
    return random.choice(USER_AGENTS)


def extract_domain(url: str) -> Optional[str]:
    """Extract root domain from URL."""
    try:
        parsed = urlparse(url)
        # Remove www. prefix if present
        domain = parsed.netloc.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except Exception:
        return None


def normalize_url(url: str) -> str:
    """Normalize URL for consistency."""
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    parsed = urlparse(url)
    # Ensure lowercase domain
    normalized = urlunparse((
        parsed.scheme,
        parsed.netloc.lower(),
        parsed.path.rstrip('/'),
        parsed.params,
        parsed.query,
        ''  # Remove fragment
    ))
    
    return normalized


def extract_email_from_text(text: str) -> Optional[str]:
    """Extract first email address from text."""
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    match = re.search(email_pattern, text)
    return match.group(0) if match else None


def extract_phone_from_text(text: str) -> Optional[str]:
    """Extract first phone number from text (US format)."""
    # Matches various US phone formats
    phone_patterns = [
        r'\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b',  # 123-456-7890
        r'\b\(\d{3}\)\s?\d{3}[-.\s]?\d{4}\b',   # (123) 456-7890
        r'\b\+1\s?\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b',  # +1 123-456-7890
    ]
    
    for pattern in phone_patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(0)
    
    return None


def clean_company_name(name: str) -> str:
    """Clean and normalize company name."""
    # Remove common suffixes
    suffixes = [
        r'\s+(?:Inc|LLC|Ltd|Limited|Corp|Corporation|Co|Company)\.?$',
        r'\s+(?:GmbH|AG|S\.A\.|S\.L\.|B\.V\.)$'
    ]
    
    cleaned = name.strip()
    for suffix in suffixes:
        cleaned = re.sub(suffix, '', cleaned, flags=re.IGNORECASE)
    
    # Remove extra whitespace
    cleaned = ' '.join(cleaned.split())
    
    return cleaned


def is_valid_domain(domain: str) -> bool:
    """Check if domain looks valid."""
    if not domain or '.' not in domain:
        return False
    
    # Basic validation
    domain_pattern = r'^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$'
    return bool(re.match(domain_pattern, domain))


def detect_captcha_block(html: str) -> bool:
    """Detect if page contains CAPTCHA challenge."""
    captcha_indicators = [
        'captcha',
        'recaptcha',
        'hcaptcha',
        'challenge-form',
        'cf-challenge',  # Cloudflare
        'verify you are human',
        'security check',
        'robot verification'
    ]
    
    html_lower = html.lower()
    return any(indicator in html_lower for indicator in captcha_indicators)


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Set up a logger with the given name."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Avoid duplicate handlers
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger
