import asyncio
import time
import uuid
import statistics
import sys
import os

# Ensure the root directory is in the python path so we can import app
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Silence structlog/app loggers to make console output clean for the test
os.environ["LOG_LEVEL"] = "ERROR"

from app.services.edge_tts import EdgeTTSService

CONCURRENCY_LEVELS = [1]
REQUESTS_PER_RUN = 1  # Number of concurrent requests to queue up per run

# Realistic Vietnamese sentences to synthesize
BASE_SENTENCES = [
    "Hôm nay trời rất đẹp, tôi muốn đi dạo quanh hồ Gươm cùng bạn bè.",
    "Hệ thống chuyển đổi văn bản thành giọng nói đang được kiểm thử hiệu năng.",
    "Microsoft Edge TTS cung cấp chất lượng giọng nói tiếng Việt khá tự nhiên.",
    "Chúng ta cần tìm ra điểm cân bằng tối ưu cho số lượng luồng xử lý đồng thời.",
    "Việc thêm chuỗi muối ngẫu nhiên giúp loại bỏ ảnh hưởng của bộ nhớ đệm máy chủ."
]

async def run_single_request(service: EdgeTTSService, index: int) -> dict:
    # Combine 4 sentences to get around 50 words, and add a random salt to bypass caching
    sentences = [BASE_SENTENCES[(index + i) % len(BASE_SENTENCES)] for i in range(4)]
    base_text = " ".join(sentences)
    salt = uuid.uuid4().hex[:8]
    text = f"{base_text} Mã kiểm tra: {salt}."
    
    start_time = time.perf_counter()
    success = False
    error = None
    queue_wait_ms = 0.0
    synth_ms = 0.0
    
    try:
        # Call the synthesis service
        audio, duration, provider, voice_id = await service.synthesize_text_bytes(
            text=text,
            voice="hoaimy",
            priority_score=0.0,
            time_to_play_ms=0.0,
            trace_id=f"stress-test-{index}"
        )
        success = True
    except Exception as e:
        error = str(e)
        
    end_time = time.perf_counter()
    duration_ms = (end_time - start_time) * 1000
    
    return {
        "index": index,
        "success": success,
        "duration_ms": duration_ms,
        "error": error
    }

async def benchmark_concurrency(concurrency: int) -> dict:
    print(f"\n>>> Starting benchmark for EDGE_TTS_CONCURRENCY = {concurrency} ...")
    
    # Initialize service with the specific concurrency
    service = EdgeTTSService(
        concurrency=concurrency,
        retries=3,  # Set to a lower value so the stress test doesn't take forever on failures
        hedge_after_attempts=0
    )
    
    # Start the workers
    await service._ensure_workers()
    
    # Queue up all requests at once to stress test the queue and workers
    start_run = time.perf_counter()
    tasks = [run_single_request(service, i) for i in range(REQUESTS_PER_RUN)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    end_run = time.perf_counter()
    
    total_time_seconds = end_run - start_run
    
    # Analyze results
    success_count = 0
    failure_count = 0
    durations = []
    errors = []
    
    for r in results:
        if isinstance(r, dict):
            if r["success"]:
                success_count += 1
                durations.append(r["duration_ms"])
            else:
                failure_count += 1
                if r["error"]:
                    errors.append(r["error"])
        else:
            # Task exception
            failure_count += 1
            errors.append(str(r))
            
    # Calculate latency statistics for successful requests
    if durations:
        avg_latency = statistics.mean(durations)
        p50_latency = statistics.median(durations)
        durations.sort()
        p95_latency = durations[int(len(durations) * 0.95)] if len(durations) >= 20 else durations[-1]
        max_latency = max(durations)
        min_latency = min(durations)
    else:
        avg_latency = p50_latency = p95_latency = max_latency = min_latency = 0.0
        
    throughput = success_count / total_time_seconds if total_time_seconds > 0 else 0.0
    
    metrics = service.get_metrics()
    proxies_info = metrics.get("proxies", {})
    
    # Clean shutdown of workers
    await service.shutdown()
    
    print(f"Finished benchmark for concurrency = {concurrency}")
    print(f"  Success: {success_count}/{REQUESTS_PER_RUN} | Failures: {failure_count}")
    print(f"  Total Time: {total_time_seconds:.2f}s | Throughput: {throughput:.2f} req/s")
    print(f"  Proxies: Active={proxies_info.get('active_count', 0)} | Dead={proxies_info.get('dead_count', 0)} | Total={proxies_info.get('total_loaded', 0)}")
    
    details = proxies_info.get("details", [])
    if details:
        print("  Individual Proxy Performance:")
        print(f"    {'Proxy (IP:Port)':<45} | {'Status':<8} | {'Reqs':<6} | {'Success':<7} | {'Failed':<6} | {'Avg Latency':<12}")
        print("    " + "-" * 95)
        for p in details:
            status_str = "DEAD" if p.get("is_dead") else "ACTIVE"
            avg_lat = f"{p.get('avg_latency_ms', 0.0):.1f} ms" if p.get("avg_latency_ms", 0.0) > 0 else "-"
            name = p.get("raw", "unknown")
            print(f"    {name:<45} | {status_str:<8} | {p.get('total_requests', 0):<6} | {p.get('successful_requests', 0):<7} | {p.get('failed_requests', 0):<6} | {avg_lat:<12}")
            
    if durations:
        print(f"  Latency (ms): Avg={avg_latency:.1f} | P50={p50_latency:.1f} | P95={p95_latency:.1f} | Max={max_latency:.1f}")
    if errors:
        # Print a sample of errors
        sample_errors = list(set(errors))[:3]
        print(f"  Errors sampled: {sample_errors}")
        
    return {
        "concurrency": concurrency,
        "success_rate": (success_count / REQUESTS_PER_RUN) * 100,
        "success_count": success_count,
        "failure_count": failure_count,
        "total_time": total_time_seconds,
        "throughput": throughput,
        "avg_latency": avg_latency,
        "p50_latency": p50_latency,
        "p95_latency": p95_latency,
        "max_latency": max_latency,
        "min_latency": min_latency,
        "errors": list(set(errors)),
        "proxies": proxies_info
    }

async def main():
    print("=" * 70)
    print("EDGE-TTS CONCURRENCY STRESS TEST")
    print("=" * 70)
    print(f"Total requests queued per concurrency level: {REQUESTS_PER_RUN}")
    print("Unique text salts will be added to bypass Microsoft server-side caching.")
    
    summaries = []
    for c in CONCURRENCY_LEVELS:
        try:
            summary = await benchmark_concurrency(c)
            summaries.append(summary)
            # Short sleep between runs to allow connection pools to clear and avoid immediate rate limiting
            await asyncio.sleep(2.0)
        except Exception as e:
            print(f"Failed to benchmark concurrency {c}: {e}")
            
    print("\n" + "=" * 70)
    print("FINAL SUMMARY REPORT")
    print("=" * 70)
    print(f"{'Concurrency':<12} | {'Success %':<10} | {'Total Time (s)':<14} | {'Throughput (req/s)':<18} | {'P50 Latency (ms)':<16} | {'P95 Latency (ms)':<16} | {'Proxies (A/D/T)':<16}")
    print("-" * 110)
    
    best_concurrency = None
    best_throughput = 0.0
    
    for s in summaries:
        p_act = s["proxies"].get("active_count", 0)
        p_dead = s["proxies"].get("dead_count", 0)
        p_tot = s["proxies"].get("total_loaded", 0)
        p_str = f"{p_act}/{p_dead}/{p_tot}"
        print(f"{s['concurrency']:<12} | {s['success_rate']:<9.1f}% | {s['total_time']:<14.2f} | {s['throughput']:<18.2f} | {s['p50_latency']:<16.1f} | {s['p95_latency']:<16.1f} | {p_str:<16}")
        
        # We define the sweet spot as the highest throughput that achieves >= 98% success rate
        if s['success_rate'] >= 98.0 and s['throughput'] > best_throughput:
            best_throughput = s['throughput']
            best_concurrency = s['concurrency']
            
    print("-" * 90)
    if best_concurrency:
        print(f"\nConclusion: The optimal sweet spot for EDGE_TTS_CONCURRENCY is: {best_concurrency}")
        print(f"It achieved {best_throughput:.2f} requests/second with a success rate of >= 98%.")
    else:
        print("\nConclusion: All concurrency levels had significant failure rates. Check server logs or Microsoft rate limits.")
    print("=" * 70)

if __name__ == "__main__":
    asyncio.run(main())
