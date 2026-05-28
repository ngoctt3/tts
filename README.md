# Remote Edge TTS Service

A production-ready, standalone, and horizontally scalable Edge TTS (Text-to-Speech) microservice.

This service synthesizes high-quality audio files using the Microsoft Edge TTS engine, stores files locally, caches metadata in an internal Redis instance, serves them instantly via Nginx, and fires robust webhook callbacks on completion.

---

## 🏗️ Architecture Overview

The system runs as a multi-container Docker Compose service:

```
                            [ Client Request ]
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │      Nginx Proxy     │
                         │      (Port 80)       │
                         └──────────┬───────────┘
               ┌────────────────────┴────────────────────┐
               ▼ (Proxy /api/)                           ▼ (Serve /media/*.mp3)
       ┌───────────────┐                          ┌───────────────┐
       │    FastAPI    ├─────────────────────────►│  Local Disk   │
       │ (TTS Engine)  │  [Saves synthesized MP3] │ (Shared Vol)  │
       └───────┬───────┘                          └───────────────┘
               │
               ▼ (Cache & Mutex Lock)
       ┌───────────────┐
       │  Local Redis  │
       │ (Independent) │
       └───────────────┘
```

1. **Nginx**: Receptors HTTP traffic.
   - Proxies `/api/` traffic to the FastAPI application.
   - Directly serves synthesized `.mp3` files from `/media/` with byte-range and CORS support.
2. **FastAPI**: The synthesis engine.
   - Validates authentication (`X-Internal-Token`).
   - Normalizes and pre-cleans text inputs (removes problematic quotes, punctuation).
   - Prevents duplicate synthesis using distributed Redis locks.
   - Employs a concurrency-limited queue for the Edge TTS library.
   - Writes audio files directly to the shared local storage volume.
   - Registers webhooks and schedules async background workers.
3. **Redis**: Cache & concurrency coordinator.
   - Caches segment metadata (URLs, durations, statuses) to prevent redundant synthesis.
   - Orchestrates locks for concurrent synthesis requests on identical keys.
   - Is strictly **internal** and does not leak/conflict with main database Redis.

---

## 🔒 Authentication

All API endpoints (except `/health` and public `/media/` files) are protected using the `X-Internal-Token` header.
The token is configured via `EDGE_TTS_INTERNAL_TOKEN` in the environment.

---

## 🚀 Getting Started

### 1. Configure the Environment
Copy the example environment file and customize the token and configuration:

```bash
cp .env.example .env
```

Key configuration variables:
- `EDGE_TTS_INTERNAL_TOKEN`: Shared secret for API authorization.
- `EDGE_TTS_PUBLIC_BASE_URL`: The public-facing URL where Nginx serves media (e.g. `http://tts.yourdomain.com/media`).
- `EDGE_TTS_CLEANUP_MAX_AGE_SECONDS`: TTL for audio files (defaults to 3 days, after which they are auto-purged to free disk space).

### 2. Launch the Service

```bash
docker compose up -d --build
```

Nginx will be exposed on port `80` (or the port defined in `docker-compose.yml`).

---

## 📡 API Reference

### 1. Synthesize Text-to-Speech Segment
* **Endpoint**: `POST /api/tts/segments`
* **Headers**:
  * `X-Internal-Token`: `<token>` (Required)
  * `X-Request-ID`: `<unique-req-id>` (Optional, will fallback to trace request id)
* **Payload Format**:

```json
{
  "cache_key": "a4f89d38c2019b87d643efaa8910d65b",
  "text": "Xin chào thế giới.",
  "voice": "hoaimy",
  "rate": "+0%",
  "volume": "+0%",
  "pitch": "+0Hz",
  "wait": true,
  "priority_score": 1.0,
  "time_to_play_ms": 0.0,
  "trace": {
    "media_index": 0,
    "time_to_play_ms": 0.0,
    "request_id": "req-12345",
    "chain_id": "chain-9999",
    "segment_index": 0,
    "kind": "content"
  },
  "webhook_url": "https://callback.myclient.com/webhook"
}
```

* **Behavior (Wait Modes)**:
  * **Synchronous (`wait: true`)**: The endpoint blocks until synthesis completes or fails. Returns a `200 OK` response with audio details.
  * **Asynchronous (`wait: false`)**: The endpoint starts synthesis in the background and immediately returns a `202 Accepted` response. Once synthesis completes, it pushes the outcome to `webhook_url`.

* **Sample Response (Ready / 200 OK)**:
```json
{
  "status": "ready",
  "cache_key": "a4f89d38c2019b87d643efaa8910d65b",
  "url": "http://localhost/media/tts/segments/a4f89d38c2019b87d643efaa8910d65b.mp3",
  "duration": 1.25,
  "bytes": 5092,
  "attempts": 1,
  "provider": "edge-tts",
  "voice": "vi-VN-HoaiMyNeural",
  "duration_source": "native_header",
  "meta_ttl_seconds": 259200,
  "error": null
}
```

---

## 🪝 Webhook Callback

When synthesis is complete (or if it fails/skips), the service sends a `POST` request to the provided `webhook_url` containing the standard response structure above.

### Reliability Features:
* **Exponential Backoff**: If the client's webhook endpoint fails (e.g. returns 5xx or experiences connection timeouts), the service automatically retries up to 5 times with exponential backoff:
  - Attempt 1: immediate retry
  - Attempt 2: 1s delay
  - Attempt 3: 2s delay
  - Attempt 4: 4s delay
  - Attempt 5: 8s delay
* **Tracing Headers**: The webhook includes tracing headers `X-Request-ID` and `X-Cache-Key` for easy routing.

---

## 🧪 Testing & Stress Tests

### Running Unit Tests
Execute the pytest suite locally to verify authentication, disk writing, caches, and webhooks:

```bash
python -m pytest tests/unit/edge_tts/test_remote_edge_tts.py
```

### Running Stress Tests
We provide a Python client utility in `scripts/stress_remote_edge_tts.py` to stress-test the deployed service, measure latency, and test webhook deliveries.

```bash
# 1. Stress test in Synchronous (wait=true) mode, 5 concurrent tasks, 20 requests total
python scripts/stress_remote_edge_tts.py --url http://127.0.0.1:8080 --token test-secret-token -c 5 -n 20

# 2. Stress test in Asynchronous (wait=false) mode, with built-in webhook receiver
python scripts/stress_remote_edge_tts.py --url http://127.0.0.1:8080 --token test-secret-token -c 10 -n 50 --async-mode

# If the service runs inside Docker and the webhook listener runs on the host,
# expose the callback as host.docker.internal.
python scripts/stress_remote_edge_tts.py --url http://127.0.0.1:8080 --token test-secret-token -c 10 -n 50 --async-mode --webhook-url http://host.docker.internal:9090/webhook
```

---

## ⚙️ Environment Variables Reference

| Variable | Default | Description |
| :--- | :--- | :--- |
| `EDGE_TTS_INTERNAL_TOKEN` | *None (Required)* | Authorization secret for APIs |
| `REDIS_URL` | `redis://redis:6379/0` | Connection string for internal Redis |
| `EDGE_TTS_PUBLIC_BASE_URL` | `http://localhost/media` | Root URL for serving MP3 files |
| `EDGE_TTS_LOCAL_DIR` | `/data/edge_tts` | Local directory for storing MP3 files |
| `EDGE_TTS_LOCAL_PREFIX` | `tts` | Path prefix inside the media directory |
| `EDGE_TTS_MAX_SEGMENT_CHARS` | `8000` | Maximum character length allowed for a single segment |
| `EDGE_TTS_CLEANUP_MAX_AGE_SECONDS` | `259200` (3 days) | Expiry limit for audio files deletion |
| `EDGE_TTS_CLEANUP_INTERVAL_SECONDS` | `3600` (1 hour) | Cleanup scan interval |
| `EDGE_TTS_WEBHOOK_TIMEOUT` | `10` | Webhook HTTP timeout (seconds) |
| `EDGE_TTS_WEBHOOK_MAX_RETRIES` | `5` | Maximum attempts for webhook delivery |
