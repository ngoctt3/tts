import asyncio
import os
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

import httpx

# Ensure root directory is in python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.proxy_manager import ProxyItem, NoProxyItem, ProxyManager
from app.services.edge_tts import EdgeTTSService


class TestProxyManager(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        # Create a temporary file with some proxies for testing loading
        self.temp_dir = tempfile.TemporaryDirectory()
        self.proxy_file_path = os.path.join(self.temp_dir.name, "test_proxies.txt")
        with open(self.proxy_file_path, "w", encoding="utf-8") as f:
            f.write("38.154.203.95:5863:barmzddg:3so7je86elbd\n")
            f.write("198.105.121.200:6462\n")
            f.write("http://64.137.96.74:6641\n")
            f.write("# comment line\n")
            f.write("\n")

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_proxy_item_parsing(self):
        # Format: ip:port:username:password
        p1 = ProxyItem("38.154.203.95:5863:barmzddg:3so7je86elbd")
        self.assertEqual(p1.url, "http://barmzddg:3so7je86elbd@38.154.203.95:5863")
        self.assertEqual(p1.raw, "38.154.203.95:5863:barmzddg:3so7je86elbd")

        # Format: ip:port
        p2 = ProxyItem("198.105.121.200:6462")
        self.assertEqual(p2.url, "http://198.105.121.200:6462")

        # Format: already URL
        p3 = ProxyItem("http://64.137.96.74:6641")
        self.assertEqual(p3.url, "http://64.137.96.74:6641")

    def test_proxy_manager_load_proxies(self):
        pm = ProxyManager(proxy_file_path=self.proxy_file_path)
        self.assertEqual(len(pm.proxies), 3)
        self.assertEqual(pm.proxies[0].url, "http://barmzddg:3so7je86elbd@38.154.203.95:5863")
        self.assertEqual(pm.proxies[1].url, "http://198.105.121.200:6462")
        self.assertEqual(pm.proxies[2].url, "http://64.137.96.74:6641")

        # 3 loaded proxies + 1 NoProxyItem = 4 active items initially
        self.assertEqual(len(pm._active_pool), 4)
        self.assertTrue(isinstance(pm._active_pool[-1], NoProxyItem))

    async def test_rotation_strategies(self):
        # Round Robin
        pm_rr = ProxyManager(proxy_file_path=self.proxy_file_path, strategy="roundrobin")
        
        # We expect items to rotate in order
        items = []
        for _ in range(4):
            item = await pm_rr.get_proxy()
            items.append(item)
            
        self.assertEqual(items[0].raw, "38.154.203.95:5863:barmzddg:3so7je86elbd")
        self.assertEqual(items[1].raw, "198.105.121.200:6462")
        self.assertEqual(items[2].url, "http://64.137.96.74:6641")
        self.assertTrue(isinstance(items[3], NoProxyItem))

        # Check next loop
        next_item = await pm_rr.get_proxy()
        self.assertEqual(next_item.raw, "38.154.203.95:5863:barmzddg:3so7je86elbd")

        # Shuffle
        pm_sh = ProxyManager(proxy_file_path=self.proxy_file_path, strategy="shuffle")
        chosen = [await pm_sh.get_proxy() for _ in range(20)]
        # Ensure we got different choices and NoProxyItem is also part of it
        self.assertTrue(any(isinstance(x, NoProxyItem) for x in chosen))
        self.assertTrue(any(x.raw == "198.105.121.200:6462" for x in chosen))

    async def test_failure_threshold_and_revival(self):
        pm = ProxyManager(proxy_file_path=self.proxy_file_path, check_interval=0.05)
        
        # Initially, active pool contains 3 loaded + 1 NoProxy
        self.assertEqual(len(pm._active_pool), 4)
        self.assertEqual(len(pm._dead_proxies), 0)

        p1 = pm.proxies[0]
        
        # First failure
        await pm.report_failure(p1)
        self.assertEqual(p1.fail_count, 1)
        self.assertEqual(len(pm._active_pool), 4)

        # Success resets count
        await pm.report_success(p1)
        self.assertEqual(p1.fail_count, 0)

        # 3 consecutive failures marks as dead
        await pm.report_failure(p1)
        await pm.report_failure(p1)
        await pm.report_failure(p1)

        self.assertEqual(p1.fail_count, 3)
        self.assertTrue(p1.is_dead)
        self.assertEqual(len(pm._active_pool), 3)  # Removed from active pool
        self.assertIn(p1, pm._dead_proxies)

        # Verify NO_PROXY is never marked dead or removed
        no_proxy = pm._no_proxy
        await pm.report_failure(no_proxy)
        await pm.report_failure(no_proxy)
        await pm.report_failure(no_proxy)
        self.assertFalse(no_proxy.is_dead)
        self.assertEqual(no_proxy.fail_count, 0)
        self.assertIn(no_proxy, pm._active_pool)

        # Start checker and test revival using mocked HTTP response
        mock_response = AsyncMock()
        mock_response.status_code = 200

        with patch("httpx.AsyncClient.get", return_value=mock_response) as mock_get:
            pm.start_checker()
            # Wait for checker to run at least once
            await asyncio.sleep(0.15)
            await pm.stop_checker()
            
            mock_get.assert_called()
            
        # P1 should be revived
        self.assertFalse(p1.is_dead)
        self.assertEqual(p1.fail_count, 0)
        self.assertEqual(len(pm._active_pool), 4)
        self.assertEqual(len(pm._dead_proxies), 0)

    async def test_no_proxy_fallback_when_all_dead(self):
        pm = ProxyManager(proxy_file_path=self.proxy_file_path)
        
        # Mark all loaded proxies as dead
        for p in list(pm.proxies):
            await pm.report_failure(p)
            await pm.report_failure(p)
            await pm.report_failure(p)
            
        self.assertEqual(len(pm._active_pool), 1)
        self.assertTrue(isinstance(pm._active_pool[0], NoProxyItem))
        
        # Even after multiple calls, it should return NoProxyItem (url=None)
        for _ in range(5):
            item = await pm.get_proxy()
            self.assertTrue(isinstance(item, NoProxyItem))
            self.assertIsNone(item.url)

    async def test_edge_tts_integration(self):
        service = EdgeTTSService(
            concurrency=2,
            retries=4,
            proxy_file=self.proxy_file_path,
            proxy_check_interval=0.1
        )
        
        # Mock _synthesize_once to fail on specific proxies
        call_count = 0
        used_proxies = []
        
        async def mock_synth_once(text, voice, rate, volume, pitch, proxy=None):
            nonlocal call_count
            call_count += 1
            used_proxies.append(proxy)
            if proxy is not None:
                # Fail all proxy connections to trigger retries and proxy failures
                raise RuntimeError("Proxy connection failed")
            # Return dummy audio on direct connection (proxy=None)
            return b"dummyaudio" * 100  # length >= 512

        with patch.object(service, "_synthesize_once", side_effect=mock_synth_once):
            audio = await service._synthesize_with_retries(
                text="Hello world",
                voice="hoaimy",
                rate="+0%",
                volume="+0%",
                pitch="+0Hz"
            )
            self.assertEqual(audio, b"dummyaudio" * 100)
            self.assertEqual(call_count, 4)
            # Three proxy URLs (strings) and one None (NO_PROXY)
            self.assertIsNone(used_proxies[-1])
            self.assertIsNotNone(used_proxies[0])
            self.assertIsNotNone(used_proxies[1])
            self.assertIsNotNone(used_proxies[2])
            
        # Ensure fail counts were recorded
        # Each used proxy should have fail_count >= 1 (consecutive)
        for proxy in service.proxy_manager.proxies:
            self.assertGreater(proxy.fail_count, 0)
            
        await service.shutdown()

    async def test_proxy_hedging_flow(self):
        service = EdgeTTSService(
            concurrency=2,
            retries=10,
            hedge_after_attempts=3,
            proxy_file=self.proxy_file_path,
            proxy_check_interval=0.1,
            proxy_hedging=True,
            proxy_hedging_depth=2,
        )
        
        # We have 3 loaded proxies in proxy_file_path
        self.assertEqual(len(service.proxy_manager.proxies), 3)
        
        used_proxies = []
        async def mock_synth_once(text, voice, rate, volume, pitch, proxy=None):
            used_proxies.append(proxy)
            raise RuntimeError("fail")
            
        with patch.object(service, "_synthesize_once", side_effect=mock_synth_once):
            early_failed_called = False
            async def on_early_failed(attempt, exc):
                nonlocal early_failed_called
                early_failed_called = True
                
            with self.assertRaises(RuntimeError):
                await service._synthesize_with_retries(
                    text="Hello hedging",
                    voice="hoaimy",
                    rate="+0%",
                    volume="+0%",
                    pitch="+0Hz",
                    on_early_failed=on_early_failed,
                )
            
            # Total attempts should be hedge_after_attempts (3) + proxy_hedging_depth (2) = 5
            self.assertEqual(len(used_proxies), 5)
            # The first 3 should be None (NO_PROXY)
            self.assertIsNone(used_proxies[0])
            self.assertIsNone(used_proxies[1])
            self.assertIsNone(used_proxies[2])
            # The next 2 should be real proxy URLs (not None)
            self.assertIsNotNone(used_proxies[3])
            self.assertIsNotNone(used_proxies[4])
            # The callback should NOT have been fired
            self.assertFalse(early_failed_called)
            
            # Verify metrics
            self.assertEqual(service.proxy_hedging_attempts, 2)
            self.assertEqual(service.proxy_hedging_successes, 0)
            self.assertEqual(service.proxy_hedging_failures, 2)
            
        await service.shutdown()


if __name__ == "__main__":
    unittest.main()
