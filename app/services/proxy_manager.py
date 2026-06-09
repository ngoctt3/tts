import asyncio
import os
import random
from collections import deque
from typing import List, Optional, Set
import httpx

from app.core.logger import get_logger


class DirectProxyItem:
    """Sentinel proxy item representing a direct (no-proxy) connection."""

    raw = "direct"
    url: Optional[str] = None
    is_dead = False
    fail_count = 0
    total_requests = 0
    successful_requests = 0
    failed_requests = 0
    latencies: deque = deque(maxlen=1)

    def __repr__(self) -> str:
        return "DirectProxyItem(direct)"


# Shared singleton — avoids creating a new object on every call
_DIRECT_PROXY = DirectProxyItem()

class ProxyItem:
    def __init__(self, raw: str):
        self.raw = raw.strip()
        parts = self.raw.split(':')
        if len(parts) == 4:
            ip, port, username, password = parts
            self.url = f"http://{username}:{password}@{ip}:{port}"
        elif len(parts) == 2:
            ip, port = parts
            self.url = f"http://{ip}:{port}"
        else:
            if self.raw.startswith("http://") or self.raw.startswith("https://"):
                self.url = self.raw
            else:
                self.url = f"http://{self.raw}"
        
        self.fail_count = 0
        self.is_dead = False
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.latencies = deque(maxlen=1000)

    def __repr__(self) -> str:
        return f"ProxyItem(raw={self.raw}, url={self.url}, fail_count={self.fail_count}, is_dead={self.is_dead})"


class ProxyManager:
    def __init__(
        self,
        proxy_file_path: str = "proxy.txt",
        strategy: str = "roundrobin",
        check_interval: float = 30.0,
        test_url: str = "http://ipconfig.me/ip",
        disable_proxy: bool = False,
    ):
        self.proxy_file_path = proxy_file_path
        self.strategy = strategy.lower()
        self.check_interval = check_interval
        self.test_url = test_url
        # When True: synthesize directly (no proxy) if pool is empty or flag is set
        self.disable_proxy = disable_proxy
        self.log = get_logger().bind(service="proxy_manager")

        self._lock = asyncio.Lock()

        # Load proxies from file (may be empty when disable_proxy=True)
        self.proxies = self.load_proxies()

        self._active_pool: List[ProxyItem] = list(self.proxies)
        self._dead_proxies: Set[ProxyItem] = set()

        self._checker_task: asyncio.Task | None = None

    def load_proxies(self) -> List[ProxyItem]:
        if not self.proxy_file_path or not os.path.exists(self.proxy_file_path):
            self.log.warning("Proxy file not found or empty path", path=self.proxy_file_path)
            return []
        
        loaded = []
        try:
            with open(self.proxy_file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        try:
                            loaded.append(ProxyItem(line))
                        except Exception as e:
                            self.log.error("Failed to parse proxy line", line=line, error=str(e))
            self.log.info("Loaded proxies from file", count=len(loaded), path=self.proxy_file_path)
        except Exception as e:
            self.log.error("Failed to read proxy file", path=self.proxy_file_path, error=str(e))
        return loaded

    def start_checker(self):
        if self._checker_task is None or self._checker_task.done():
            self._checker_task = asyncio.create_task(
                self._check_dead_proxies_loop(), name="proxy-checker-task"
            )
            self.log.info("Started proxy checker background task")

    async def stop_checker(self):
        if self._checker_task is not None:
            self._checker_task.cancel()
            try:
                await self._checker_task
            except asyncio.CancelledError:
                pass
            self._checker_task = None
            self.log.info("Stopped proxy checker background task")

    async def get_proxy(self) -> "ProxyItem | DirectProxyItem":
        """Return the next proxy from the active pool.

        Falls back to the *direct* sentinel when:
        - ``disable_proxy`` is True (DISABLE_PROXY env var), OR
        - The active pool is empty and ``disable_proxy`` is True.

        Raises ``RuntimeError`` when the pool is empty AND ``disable_proxy``
        is False (original behaviour — keeps the hard failure explicit).
        """
        if self.disable_proxy:
            return _DIRECT_PROXY

        async with self._lock:
            if not self._active_pool:
                # All proxies are dead → fallback to direct if allowed
                self.log.warning(
                    "proxy_pool_exhausted_fallback_direct",
                    total=len(self.proxies),
                    dead=len(self._dead_proxies),
                )
                return _DIRECT_PROXY

            if self.strategy == "shuffle":
                return random.choice(self._active_pool)
            else:  # roundrobin
                item = self._active_pool.pop(0)
                self._active_pool.append(item)
                return item

    async def report_success(self, proxy: "ProxyItem | DirectProxyItem", latency: float = 0.0):
        if isinstance(proxy, DirectProxyItem):
            return  # Nothing to track for direct connections
        async with self._lock:
            proxy.total_requests += 1
            proxy.successful_requests += 1
            if latency > 0.0:
                proxy.latencies.append(latency)

            if proxy.fail_count > 0:
                self.log.debug("Resetting proxy fail count on success", proxy=proxy.raw)
                proxy.fail_count = 0

    async def report_failure(self, proxy: "ProxyItem | DirectProxyItem", latency: float = 0.0):
        if isinstance(proxy, DirectProxyItem):
            return  # Direct connections are not tracked
        async with self._lock:
            proxy.total_requests += 1
            proxy.failed_requests += 1
            if latency > 0.0:
                proxy.latencies.append(latency)

            # A single configured endpoint may be a rotating proxy gateway.
            # One failed exit IP must not remove the only usable endpoint.
            if len(self.proxies) == 1:
                proxy.fail_count = 0
                proxy.is_dead = False
                self._dead_proxies.discard(proxy)
                if proxy not in self._active_pool:
                    self._active_pool.append(proxy)
                self.log.warning("Single rotating proxy failed; keeping it active", proxy=proxy.raw)
                return

            proxy.fail_count += 1
            self.log.warning("Proxy failed", proxy=proxy.raw, fail_count=proxy.fail_count)
            if proxy.fail_count >= 3:
                proxy.is_dead = True
                if proxy in self._active_pool:
                    self._active_pool.remove(proxy)
                self._dead_proxies.add(proxy)
                self.log.error(
                    "Proxy marked as DEAD and moved to health checking pool",
                    proxy=proxy.raw,
                    consecutive_failures=proxy.fail_count,
                )

    async def _test_proxy(self, proxy: ProxyItem) -> bool:
        try:
            async with httpx.AsyncClient(proxy=proxy.url, timeout=5.0) as client:
                response = await client.get(self.test_url)
                if response.status_code == 200:
                    return True
        except Exception as e:
            self.log.debug("Proxy health check failed", proxy=proxy.raw, error=str(e))
        return False

    async def _check_dead_proxies_loop(self):
        while True:
            try:
                await asyncio.sleep(self.check_interval)
                
                async with self._lock:
                    dead_list = list(self._dead_proxies)
                
                if not dead_list:
                    continue

                self.log.info("Checking dead proxies health", count=len(dead_list))
                # Check dead proxies concurrently
                tasks = [self._test_proxy(p) for p in dead_list]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for proxy, success in zip(dead_list, results):
                    if success is True:
                        async with self._lock:
                            if proxy in self._dead_proxies:
                                self._dead_proxies.remove(proxy)
                                proxy.fail_count = 0
                                proxy.is_dead = False
                                if proxy not in self._active_pool:
                                    self._active_pool.append(proxy)
                                self.log.success("Proxy revived and returned to pool", proxy=proxy.raw)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error("Error in dead proxy checker loop", error=str(e))
