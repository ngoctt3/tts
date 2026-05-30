import asyncio
import os
import sys
import random
import tempfile
import time

# Ensure root directory is in python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.edge_tts import EdgeTTSService
from app.services.proxy_manager import ProxyItem, NoProxyItem

async def run_stress_test():
    print("=== Starting Proxy Stress Test ===")
    
    # 1. Create a dummy proxy list
    temp_dir = tempfile.TemporaryDirectory()
    proxy_file_path = os.path.join(temp_dir.name, "stress_proxies.txt")
    
    num_proxies = 10
    proxies_written = []
    with open(proxy_file_path, "w", encoding="utf-8") as f:
        for i in range(num_proxies):
            # Form: ip:port:username:password
            # We'll use dummy credentials and IPs
            p_str = f"192.168.1.{10+i}:{8000+i}:user{i}:pass{i}"
            f.write(f"{p_str}\n")
            proxies_written.append(p_str)
            
    print(f"Created {num_proxies} mock proxies in {proxy_file_path}")

    # 2. Setup EdgeTTSService with custom proxy config
    # We set retries high so that concurrent tasks can keep retrying until they succeed
    service = EdgeTTSService(
        concurrency=16,
        retries=15,
        proxy_file=proxy_file_path,
        proxy_strategy="roundrobin",
        proxy_check_interval=1.0,  # fast check interval for stress test
    )
    
    # Track statistics of mock calls
    stats = {
        "calls": 0,
        "success": 0,
        "failures": 0,
        "by_proxy": {}
    }
    
    # Mock _synthesize_once to simulate real proxy behaviors:
    # - Some proxies are extremely flaky (always fail)
    # - Some are moderately flaky (fail 50% of the time)
    # - Direct connection (proxy=None) always succeeds
    # - Normal proxies fail 20% of the time
    async def mock_synthesize_once(text, voice, rate, volume, pitch, proxy=None):
        stats["calls"] += 1
        stats["by_proxy"].setdefault(proxy, {"calls": 0, "success": 0, "fail": 0})
        stats["by_proxy"][proxy]["calls"] += 1
        
        # Simulate network latency
        await asyncio.sleep(random.uniform(0.01, 0.05))
        
        if proxy is None:
            stats["success"] += 1
            stats["by_proxy"][proxy]["success"] += 1
            return b"a" * 1024
            
        # Determine failure rate based on proxy IP
        # Let's say odd IPs fail 80% of the time, even IPs fail 20% of the time
        fail_threshold = 0.20
        if "192.168.1.1" in proxy:  # e.g., 192.168.1.11, .13, .15, .17, .19
            # odd last digit
            last_digit = int(proxy.split("@")[1].split(":")[0].split(".")[-1])
            if last_digit % 2 != 0:
                fail_threshold = 0.80

        if random.random() < fail_threshold:
            stats["failures"] += 1
            stats["by_proxy"][proxy]["fail"] += 1
            raise RuntimeError("Mock proxy connection error")
            
        stats["success"] += 1
        stats["by_proxy"][proxy]["success"] += 1
        return b"a" * 1024

    # Mock the internal function
    service._synthesize_once = mock_synthesize_once
    
    # Mock the proxy manager's _test_proxy method to simulate health check revival:
    # Let's say health check succeeds 50% of the time, so dead proxies can revive
    async def mock_test_proxy(proxy: ProxyItem) -> bool:
        await asyncio.sleep(0.05)
        # 50% chance to revive
        revives = random.random() < 0.50
        print(f"[Health Check] Testing dead proxy {proxy.raw} -> {'REVIVED' if revives else 'STILL DEAD'}")
        return revives
        
    service.proxy_manager._test_proxy = mock_test_proxy

    # Start the service workers and background tasks
    await service._ensure_workers()
    
    # 3. Spawn concurrent tasks
    num_requests = 100
    print(f"Spawning {num_requests} concurrent requests on {service.concurrency} worker threads...")
    
    async def worker_job(idx):
        try:
            # Random priority
            audio, duration, dur_src, voice_id = await service.synthesize_text_bytes(
                text=f"Stress text segment {idx}",
                priority_score=random.uniform(0.0, 10.0),
                time_to_play_ms=100.0,
            )
            return idx, True, len(audio)
        except Exception as e:
            return idx, False, str(e)

    start_time = time.perf_counter()
    jobs = [worker_job(i) for i in range(num_requests)]
    results = await asyncio.gather(*jobs)
    elapsed = time.perf_counter() - start_time
    
    # 4. Print stats and verification
    success_jobs = [r for r in results if r[1] is True]
    failed_jobs = [r for r in results if r[1] is False]
    
    print(f"\n=== Stress Test Results ===")
    print(f"Completed in {elapsed:.3f} seconds")
    print(f"Total Requests: {num_requests}")
    print(f"Successful Requests: {len(success_jobs)}")
    print(f"Failed Requests: {len(failed_jobs)}")
    print(f"Total Mock Synth Calls: {stats['calls']}")
    print(f"Mock Synth Successes: {stats['success']}")
    print(f"Mock Synth Failures: {stats['failures']}")
    
    print("\nProxy Pool Status:")
    async with service.proxy_manager._lock:
        print(f"Active Pool Size: {len(service.proxy_manager._active_pool)}")
        for idx, item in enumerate(service.proxy_manager._active_pool):
            print(f"  [{idx}] {item.raw} (fail_count={item.fail_count})")
        print(f"Dead Pool Size: {len(service.proxy_manager._dead_proxies)}")
        for item in service.proxy_manager._dead_proxies:
            print(f"  [DEAD] {item.raw} (fail_count={item.fail_count})")

    # Assertions
    assert len(failed_jobs) == 0, f"Some jobs failed: {failed_jobs}"
    assert len(success_jobs) == num_requests, "Not all requests completed successfully"
    print("\n✓ ALL concurrent requests completed successfully!")
    
    # Stop service
    await service.shutdown()
    temp_dir.cleanup()
    print("=== Stress Test Completed Successfully ===")

if __name__ == "__main__":
    asyncio.run(run_stress_test())
