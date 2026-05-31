import asyncio
import os
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

import httpx

# Ensure root directory is in python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.proxy_manager import ProxyItem, ProxyManager
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

        # Active pool contains real proxies only.
        self.assertEqual(len(pm._active_pool), 3)

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
        self.assertEqual(items[3].raw, "38.154.203.95:5863:barmzddg:3so7je86elbd")

        # Check next loop
        next_item = await pm_rr.get_proxy()
        self.assertEqual(next_item.raw, "198.105.121.200:6462")

        # Shuffle
        pm_sh = ProxyManager(proxy_file_path=self.proxy_file_path, strategy="shuffle")
        chosen = [await pm_sh.get_proxy() for _ in range(20)]
        # Ensure we got real proxy choices only.
        self.assertTrue(any(x.raw == "198.105.121.200:6462" for x in chosen))

    async def test_failure_threshold_and_revival(self):
        pm = ProxyManager(proxy_file_path=self.proxy_file_path, check_interval=0.05)
        
        # Initially, active pool contains the 3 loaded proxies only.
        self.assertEqual(len(pm._active_pool), 3)
        self.assertEqual(len(pm._dead_proxies), 0)

        p1 = pm.proxies[0]
        
        # First failure
        await pm.report_failure(p1)
        self.assertEqual(p1.fail_count, 1)
        self.assertEqual(len(pm._active_pool), 3)

        # Success resets count
        await pm.report_success(p1)
        self.assertEqual(p1.fail_count, 0)

        # 3 consecutive failures marks as dead
        await pm.report_failure(p1)
        await pm.report_failure(p1)
        await pm.report_failure(p1)

        self.assertEqual(p1.fail_count, 3)
        self.assertTrue(p1.is_dead)
        self.assertEqual(len(pm._active_pool), 2)  # Removed from active pool
        self.assertIn(p1, pm._dead_proxies)

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
        self.assertEqual(len(pm._active_pool), 3)
        self.assertEqual(len(pm._dead_proxies), 0)

    async def test_raises_when_all_proxies_dead(self):
        pm = ProxyManager(proxy_file_path=self.proxy_file_path)
        
        # Mark all loaded proxies as dead
        for p in list(pm.proxies):
            await pm.report_failure(p)
            await pm.report_failure(p)
            await pm.report_failure(p)
            
        self.assertEqual(len(pm._active_pool), 0)
        
        with self.assertRaisesRegex(RuntimeError, "No active proxies available"):
            await pm.get_proxy()

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
            if call_count < 4:
                raise RuntimeError("Proxy connection failed")
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
            self.assertTrue(all(proxy is not None for proxy in used_proxies))
            
        # Ensure all attempts were recorded against real proxies.
        self.assertEqual(sum(proxy.total_requests for proxy in service.proxy_manager.proxies), 4)
        self.assertEqual(sum(proxy.failed_requests for proxy in service.proxy_manager.proxies), 3)
            
        await service.shutdown()

    async def test_proxy_hedging_flow(self):
        service = EdgeTTSService(
            concurrency=2,
            retries=10,
            hedge_after_attempts=3,
            proxy_file=self.proxy_file_path,
            proxy_check_interval=0.1,
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
            # All attempts should use real proxy URLs.
            self.assertTrue(all(proxy is not None for proxy in used_proxies))
            # The callback should NOT have been fired
            self.assertFalse(early_failed_called)
            
            # Verify metrics
            self.assertEqual(service.proxy_hedging_attempts, 5)
            self.assertEqual(service.proxy_hedging_successes, 0)
            self.assertEqual(service.proxy_hedging_failures, 5)
            
        await service.shutdown()

    def test_empty_latency_metrics_include_percentiles(self):
        service = EdgeTTSService(proxy_file=self.proxy_file_path)
        empty_summary = {"count": 0, "p50": 0.0, "p95": 0.0, "max": 0.0, "avg": 0.0}

        self.assertEqual(service._latency_summary([]), empty_summary)

        metrics = service.get_metrics()
        self.assertEqual(metrics["latency_ms"]["queue_wait"], empty_summary)
        self.assertEqual(metrics["latency_ms"]["synthesize"], empty_summary)
        self.assertEqual(metrics["latency_ms"]["duration_scan"], empty_summary)
        self.assertEqual(metrics["latency_ms"]["synthesize"]["p95"], 0.0)


if __name__ == "__main__":
    unittest.main()
