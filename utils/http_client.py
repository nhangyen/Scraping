import threading
import time
import random
import logging
from dataclasses import dataclass
from typing import Dict, Optional

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from urllib.parse import urlparse
from urllib import robotparser


# A small pool of realistic desktop/mobile user agents
USER_AGENTS = [
    # Chrome Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    # Chrome Android
    "Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36",
    # Safari macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    # Firefox Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]


@dataclass
class HttpClientConfig:
    max_rps: float = 1.5  # max requests per second per host (default 1 req / 2s)
    timeout: float = 15.0
    retry_total: int = 5
    retry_backoff: float = 0.5
    rotate_user_agent: bool = True
    respect_robots: bool = False
    proxy: Optional[str] = None  # e.g. http://user:pass@host:port
    # connection pool sizes for higher concurrency without opening too many sockets
    pool_connections: int = 50
    pool_maxsize: int = 100


class RateLimiter:
    """Simple per-host rate limiter enforcing a minimum interval between requests."""

    def __init__(self, min_interval: float):
        self.min_interval = max(0.0, float(min_interval))
        self._lock = threading.Lock()
        self._host_last_ts: Dict[str, float] = {}

    def wait(self, host: str):
        if self.min_interval <= 0:
            return
        with self._lock:
            now = time.time()
            last = self._host_last_ts.get(host, 0.0)
            delta = now - last
            sleep_for = self.min_interval - delta
            if sleep_for > 0:
                # add a tiny jitter to avoid burst alignment
                time.sleep(sleep_for + random.uniform(0.05, 0.2))
            self._host_last_ts[host] = time.time()


class HttpClient:
    def __init__(self, logger: Optional[logging.Logger] = None, config: Optional[HttpClientConfig] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.config = config or HttpClientConfig()

        self.session = requests.Session()

        # Retry strategy for transient errors and HTTP 429/5xx
        retry = Retry(
            total=self.config.retry_total,
            backoff_factor=self.config.retry_backoff,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=self.config.pool_connections,
            pool_maxsize=self.config.pool_maxsize,
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Default headers
        ua = random.choice(USER_AGENTS)
        self.session.headers.update({
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        })

        # Proxy support (single string applied to both http/https)
        if self.config.proxy:
            self.session.proxies.update({
                "http": self.config.proxy,
                "https": self.config.proxy,
            })

        # Per-host rate limiter
        min_interval = 1.0 / self.config.max_rps if self.config.max_rps > 0 else 0.0
        self.rate_limiter = RateLimiter(min_interval=min_interval)

        # robots.txt parsers cache per host
        self._robots_cache: Dict[str, robotparser.RobotFileParser] = {}
        self._robots_lock = threading.Lock()

    def _get_host(self, url: str) -> str:
        return urlparse(url).netloc

    def _can_fetch(self, url: str) -> bool:
        if not self.config.respect_robots:
            return True
        host = self._get_host(url)
        with self._robots_lock:
            rp = self._robots_cache.get(host)
            if rp is None:
                robots_url = f"https://{host}/robots.txt"
                rp = robotparser.RobotFileParser()
                try:
                    rp.set_url(robots_url)
                    rp.read()
                except Exception:
                    # If robots can't be fetched, be conservative: allow
                    pass
                self._robots_cache[host] = rp
        try:
            return rp.can_fetch(self.session.headers.get("User-Agent", "*"), url)
        except Exception:
            return True

    def _maybe_rotate_user_agent(self):
        if not self.config.rotate_user_agent:
            return
        # 10% chance to rotate UA between requests
        if random.random() < 0.1:
            ua = random.choice(USER_AGENTS)
            self.session.headers["User-Agent"] = ua

    def get(self, url: str, timeout: Optional[float] = None, headers: Optional[dict] = None) -> requests.Response:
        if not self._can_fetch(url):
            raise PermissionError(f"Blocked by robots.txt: {url}")

        self._maybe_rotate_user_agent()
        host = self._get_host(url)
        self.rate_limiter.wait(host)

        # Merge headers if provided
        request_headers = None
        if headers:
            request_headers = {**self.session.headers, **headers}

        resp = self.session.get(url, headers=request_headers, timeout=timeout or self.config.timeout)
        resp.raise_for_status()
        return resp
