"""Remote Edge TTS Service — Standalone FastAPI application.

Accepts TTS segment requests via HTTP, synthesizes audio using edge-tts,
stores MP3 files locally on disk (served by Nginx), caches metadata in
an internal Redis instance, and fires webhook callbacks on completion.

Designed to run as an independent Docker service on cheap VPS nodes
and scale horizontally by deploying multiple nodes behind a load balancer.
"""

import asyncio
import os
import re
import time
import threading
from contextlib import asynccontextmanager
from pathlib import Path, PurePosixPath
from typing import Any, Literal, Optional
from urllib.parse import quote

import httpx
import redis.asyncio as redis
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.core.logger import get_logger
from app.services.edge_tts import VOICE_ALIASES, edge_tts_service


log = get_logger().bind(service="remote_edge_tts")

# ──────────────────────────────────────────────────────────────────────
# Environment helpers
# ──────────────────────────────────────────────────────────────────────

def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return float(value)
    except ValueError:
        return default


# ──────────────────────────────────────────────────────────────────────
# Configuration from environment
# ──────────────────────────────────────────────────────────────────────

INTERNAL_TOKEN = os.getenv("EDGE_TTS_INTERNAL_TOKEN", "").strip()
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0").strip()
PUBLIC_BASE_URL = os.getenv("EDGE_TTS_PUBLIC_BASE_URL", "http://localhost/media").strip().rstrip("/")

MAX_SEGMENT_CHARS = _env_int("EDGE_TTS_MAX_SEGMENT_CHARS", 8000)
FAILED_TEXT_LOG_CHARS = _env_int("EDGE_TTS_FAILED_TEXT_LOG_CHARS", 1200)
FORCE_FAIL_SEGMENT_INDEX = _env_int("EDGE_TTS_FORCE_FAIL_SEGMENT_INDEX", -1)
SEGMENT_META_TTL_SECONDS = _env_int("EDGE_TTS_SEGMENT_META_TTL_SECONDS", 3 * 24 * 3600)
SEGMENT_LOCK_SECONDS = _env_int("EDGE_TTS_SEGMENT_LOCK_SECONDS", 180)
METRICS_CACHE_SECONDS = _env_float("EDGE_TTS_METRICS_CACHE_SECONDS", 2.0)

# Local storage
LOCAL_DIR = os.getenv("EDGE_TTS_LOCAL_DIR", "/data/edge_tts")
LOCAL_PREFIX = os.getenv("EDGE_TTS_LOCAL_PREFIX", "tts").strip("/")

# Cleanup
CLEANUP_MAX_AGE_SECONDS = _env_int("EDGE_TTS_CLEANUP_MAX_AGE_SECONDS", 3 * 24 * 3600)
CLEANUP_INTERVAL_SECONDS = _env_int("EDGE_TTS_CLEANUP_INTERVAL_SECONDS", 3600)

# Ensure Redis key TTL is strictly less than cleanup max age to prevent serving cached metadata for deleted files
if SEGMENT_META_TTL_SECONDS >= CLEANUP_MAX_AGE_SECONDS:
    buffer = max(1, int(CLEANUP_MAX_AGE_SECONDS * 0.1))
    adjusted_ttl = max(1, CLEANUP_MAX_AGE_SECONDS - buffer)
    log.warning(
        "adjusting_segment_meta_ttl",
        original_ttl=SEGMENT_META_TTL_SECONDS,
        cleanup_max_age=CLEANUP_MAX_AGE_SECONDS,
        adjusted_ttl=adjusted_ttl,
        reason="Redis key TTL must be less than cleanup max age to avoid stale cached links",
    )
    SEGMENT_META_TTL_SECONDS = adjusted_ttl

# Webhook
WEBHOOK_TIMEOUT = _env_int("EDGE_TTS_WEBHOOK_TIMEOUT", 10)
WEBHOOK_MAX_RETRIES = _env_int("EDGE_TTS_WEBHOOK_MAX_RETRIES", 5)

# Text pre-cleaning
TTS_QUOTE_CHARS = "'\"`´‘’‚‛“”„‟‹›«»＇＂｀〝〞〟〃「」『』《》〈〉"
TTS_QUOTE_TRANSLATION = str.maketrans({char: "" for char in TTS_QUOTE_CHARS})

# ──────────────────────────────────────────────────────────────────────
# Local disk storage (simplified, no R2)
# ──────────────────────────────────────────────────────────────────────

class LocalDiskStorage:
    def __init__(self, root_dir: str, public_base_url: str, prefix: str):
        self.root_dir = Path(root_dir).resolve()
        self.public_base_url = public_base_url.rstrip("/")
        self.prefix = prefix.strip("/")
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def upload_bytes(self, data: bytes, key: str) -> None:
        target = self._path_for_key(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f".{target.name}.{threading.get_ident()}.{time.time_ns()}.tmp")
        with tmp.open("wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, target)

    def url(self, key: str) -> str:
        clean = self._clean_key(key)
        return f"{self.public_base_url}/{quote(clean, safe='/')}"

    def exists(self, key: str) -> bool:
        return self._path_for_key(key).is_file()

    def _path_for_key(self, key: str) -> Path:
        clean_key = self._clean_key(key)
        target = (self.root_dir / Path(*clean_key.split("/"))).resolve()
        if target != self.root_dir and self.root_dir not in target.parents:
            raise ValueError("local storage key escapes root directory")
        return target

    @staticmethod
    def _clean_key(key: str) -> str:
        value = (key or "").replace("\\", "/").strip().lstrip("/")
        parts = PurePosixPath(value).parts
        if not value or any(part in {"", ".", ".."} for part in parts):
            raise ValueError("invalid local storage key")
        return "/".join(parts)


media_storage = LocalDiskStorage(
    root_dir=LOCAL_DIR,
    public_base_url=PUBLIC_BASE_URL,
    prefix=LOCAL_PREFIX,
)

# ──────────────────────────────────────────────────────────────────────
# Global state
# ──────────────────────────────────────────────────────────────────────

redis_pool: redis.Redis | None = None
metrics_cache: dict = {"expires_at": 0.0, "payload": None}
background_segment_tasks: set[asyncio.Task] = set()

# ──────────────────────────────────────────────────────────────────────
# Pydantic models
# ──────────────────────────────────────────────────────────────────────

class TTSSegmentTrace(BaseModel):
    media_index: int
    time_to_play_ms: float
    request_id: str = ""
    chain_id: str = ""
    chapter_number: int | None = None
    segment_index: int | None = None
    kind: str = ""


class TTSSegmentRequest(BaseModel):
    cache_key: str = Field(..., min_length=16, max_length=128)
    text: str = Field(..., min_length=1)
    voice: str = Field("hoaimy")
    rate: str = "+0%"
    volume: str = "+0%"
    pitch: str = "+0Hz"
    priority_score: float = 0.0
    time_to_play_ms: float = 0.0
    trace: TTSSegmentTrace
    webhook_url: str | None = Field(default=None, max_length=2048)


class TTSSegmentResponse(BaseModel):
    status: Literal["ready", "generating", "deferred", "dead_letter", "skipped", "failed", "early_failed"]
    cache_key: str
    url: str | None = None
    duration: float | None = None
    bytes: int | None = None
    attempts: int | None = None
    provider: str = "edge-tts"
    voice: str | None = None
    duration_source: str | None = None
    meta_ttl_seconds: int = SEGMENT_META_TTL_SECONDS
    error: str | None = None


# ──────────────────────────────────────────────────────────────────────
# Auth dependency
# ──────────────────────────────────────────────────────────────────────

async def require_internal_token(
    x_internal_token: Optional[str] = Header(default=None, alias="X-Internal-Token"),
) -> None:
    if not INTERNAL_TOKEN:
        raise HTTPException(status_code=503, detail="EDGE_TTS_INTERNAL_TOKEN is not configured")
    if x_internal_token != INTERNAL_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid internal token")


# ──────────────────────────────────────────────────────────────────────
# File cleanup task
# ──────────────────────────────────────────────────────────────────────

def _sync_cleanup_files(directory: str, max_age_seconds: int) -> tuple[int, int]:
    cutoff = time.time() - max_age_seconds
    root = Path(directory)
    deleted_count = 0
    total_bytes_freed = 0
    log.info(f"Cleaning up files in {directory} older than {max_age_seconds} seconds")
    if not root.exists():
        return 0, 0
    for mp3_file in root.rglob("*.mp3"):
        try:
            stat = mp3_file.stat()
            if stat.st_mtime < cutoff:
                file_size = stat.st_size
                mp3_file.unlink(missing_ok=True)
                deleted_count += 1
                total_bytes_freed += file_size
        except OSError:
            pass
    return deleted_count, total_bytes_freed


async def _cleanup_old_files() -> None:
    """Periodically scan media directory and delete files older than CLEANUP_MAX_AGE_SECONDS."""
    log.info(f"Starting cleanup task with interval {CLEANUP_INTERVAL_SECONDS} seconds")
    while True:
        try:
            await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)
            deleted_count, total_bytes_freed = await asyncio.to_thread(
                _sync_cleanup_files, LOCAL_DIR, CLEANUP_MAX_AGE_SECONDS
            )
            if deleted_count > 0:
                log.success(
                    "edge_tts_cleanup_completed",
                    deleted_files=deleted_count,
                    freed_bytes=total_bytes_freed,
                    freed_mb=round(total_bytes_freed / 1024 / 1024, 2),
                    max_age_seconds=CLEANUP_MAX_AGE_SECONDS,
                )
        except asyncio.CancelledError:
            break
        except Exception as exc:
            log.warning("edge_tts_cleanup_error", error=str(exc))


# ──────────────────────────────────────────────────────────────────────
# Webhook sender
# ──────────────────────────────────────────────────────────────────────

async def _send_webhook(
    webhook_url: str,
    payload: dict,
    *,
    request_id: str,
    cache_key: str,
) -> None:
    """POST the payload to the webhook URL with exponential backoff retry."""
    for attempt in range(1, WEBHOOK_MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=WEBHOOK_TIMEOUT) as client:
                response = await client.post(
                    webhook_url,
                    json=payload,
                    headers={
                        "Content-Type": "application/json",
                        "X-Request-ID": request_id,
                        "X-Cache-Key": cache_key,
                    },
                )
                if response.status_code < 400:
                    # log.info(
                    #     "edge_tts_webhook_sent",
                    #     request_id=request_id,
                    #     cache_key=cache_key,
                    #     status_code=response.status_code,
                    #     attempt=attempt,
                    # )
                    return
                log.warning(
                    "edge_tts_webhook_http_error",
                    request_id=request_id,
                    cache_key=cache_key,
                    status_code=response.status_code,
                    attempt=attempt,
                )
        except Exception as exc:
            log.warning(
                "edge_tts_webhook_failed",
                request_id=request_id,
                cache_key=cache_key,
                attempt=attempt,
                max_retries=WEBHOOK_MAX_RETRIES,
                error=str(exc),
            )
        if attempt < WEBHOOK_MAX_RETRIES:
            await asyncio.sleep(min(30, 0.5 * (2 ** (attempt - 1))))

    log.error(
        "edge_tts_webhook_exhausted",
        request_id=request_id,
        cache_key=cache_key,
        webhook_url=webhook_url[:200],
        max_retries=WEBHOOK_MAX_RETRIES,
    )


def _fire_webhook_background(
    webhook_url: str | None,
    response: TTSSegmentResponse,
    *,
    request_id: str,
) -> None:
    """Schedule a background webhook if webhook_url is provided."""
    if not webhook_url:
        return
    payload = response.model_dump(mode="json")
    task = asyncio.create_task(
        _send_webhook(webhook_url, payload, request_id=request_id, cache_key=response.cache_key)
    )
    _track_background_task(task)


# ──────────────────────────────────────────────────────────────────────
# Lifespan
# ──────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    cleanup_task = asyncio.create_task(_cleanup_old_files())
    # log.info(
    #     "remote_edge_tts_started",
    #     public_base_url=PUBLIC_BASE_URL,
    #     cleanup_interval_seconds=CLEANUP_INTERVAL_SECONDS,
    #     cleanup_max_age_seconds=CLEANUP_MAX_AGE_SECONDS,
    #     concurrency=edge_tts_service.concurrency,
    # )
    try:
        yield
    finally:
        cleanup_task.cancel()
        tasks = list(background_segment_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await edge_tts_service.shutdown()
        if redis_pool is not None:
            await redis_pool.aclose()


# ──────────────────────────────────────────────────────────────────────
# FastAPI App
# ──────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Remote Edge TTS Service",
    version="1.0.0",
    lifespan=lifespan,
    docs_url=None if os.getenv("ENV", "production").lower() == "production" else "/docs",
    redoc_url=None,
    openapi_url=None if os.getenv("ENV", "production").lower() == "production" else "/openapi.json",
)


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────

def _request_id_header(value: Optional[str]) -> str:
    return (value or "").strip() or "-"


def _trace_id(request_id: str, chain_id: str | None = None, segment_index: int | None = None) -> str:
    parts = [request_id]
    if chain_id:
        parts.append(chain_id)
    if segment_index is not None:
        parts.append(str(segment_index))
    return ":".join(parts)


def _preclean_tts_text(text: str) -> str:
    return " ".join((text or "").translate(TTS_QUOTE_TRANSLATION).split())


def _failed_text_preview(text: str) -> str:
    limit = max(0, FAILED_TEXT_LOG_CHARS)
    value = " ".join((text or "").split())
    if limit <= 0 or len(value) <= limit:
        return value
    return value[:limit] + "...[truncated]"


def _has_speakable_text(text_value: str) -> bool:
    normalized = re.sub(r"\s+", " ", (text_value or "")).strip()
    return any(char.isalnum() for char in normalized)


def _should_force_segment_failure(trace: TTSSegmentTrace) -> bool:
    return FORCE_FAIL_SEGMENT_INDEX >= 0 and trace.segment_index == FORCE_FAIL_SEGMENT_INDEX


def _generating_response(cache_key: str, request_id: str) -> JSONResponse:
    return JSONResponse(
        status_code=202,
        content=TTSSegmentResponse(status="generating", cache_key=cache_key).model_dump(),
        headers={"Cache-Control": "no-store", "X-Request-ID": request_id},
    )


def _track_background_task(task: asyncio.Task) -> None:
    background_segment_tasks.add(task)

    def _done(done_task: asyncio.Task) -> None:
        background_segment_tasks.discard(done_task)
        try:
            done_task.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.warning("edge_tts_background_task_failed", error=str(exc))

    task.add_done_callback(_done)


def _segment_storage_key(cache_key: str) -> str:
    prefix = LOCAL_PREFIX
    if prefix:
        return f"{prefix}/segments/{cache_key}.mp3"
    return f"segments/{cache_key}.mp3"


# ──────────────────────────────────────────────────────────────────────
# Redis helpers
# ──────────────────────────────────────────────────────────────────────

async def _segment_redis() -> redis.Redis:
    global redis_pool
    if not REDIS_URL:
        raise HTTPException(status_code=503, detail="REDIS_URL is not configured")
    if redis_pool is None:
        redis_pool = redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
        await redis_pool.ping()
    return redis_pool


def _segment_meta_key(cache_key: str) -> str:
    return f"remote_tts:segment:{cache_key}"


def _segment_lock_key(cache_key: str) -> str:
    return f"remote_tts:lock:{cache_key}"


async def _write_segment_meta(client: redis.Redis, cache_key: str, meta: dict[str, Any]) -> None:
    pipe = client.pipeline()
    pipe.hset(_segment_meta_key(cache_key), mapping=meta)
    pipe.expire(_segment_meta_key(cache_key), SEGMENT_META_TTL_SECONDS)
    await pipe.execute()


async def _release_segment_lock(client: redis.Redis, lock_key: str, lock_token: str) -> None:
    if await client.get(lock_key) == lock_token:
        await client.delete(lock_key)


def _segment_response_from_meta(cache_key: str, meta: dict[str, Any]) -> TTSSegmentResponse:
    def as_float(value: Any) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def as_int(value: Any) -> int | None:
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None

    status = str(meta.get("status") or "generating")
    if status not in {"ready", "generating", "deferred", "dead_letter", "skipped", "failed", "early_failed"}:
        status = "generating"
    return TTSSegmentResponse(
        status=status,  # type: ignore[arg-type]
        cache_key=cache_key,
        url=meta.get("url"),
        duration=as_float(meta.get("duration")),
        bytes=as_int(meta.get("bytes")),
        attempts=as_int(meta.get("attempts")),
        provider=meta.get("provider") or "edge-tts",
        voice=meta.get("voice"),
        duration_source=meta.get("duration_source"),
        meta_ttl_seconds=SEGMENT_META_TTL_SECONDS,
        error=meta.get("last_error"),
    )


# ──────────────────────────────────────────────────────────────────────
# Synthesis logic
# ──────────────────────────────────────────────────────────────────────

async def _synthesize_segment_to_meta(
    *,
    body: TTSSegmentRequest,
    client: redis.Redis,
    request_id: str,
    trace_id: str,
    text: str,
    storage_key: str,
    started_at: float,
) -> TTSSegmentResponse:
    trace = body.trace
    try:
        if _should_force_segment_failure(trace):
            raise RuntimeError(f"Forced Edge TTS test failure for segment_index={trace.segment_index}")

        if not _has_speakable_text(text):
            meta = {
                "status": "skipped",
                "cache_key": body.cache_key,
                "duration": "0.000",
                "duration_source": "unspeakable_text",
                "bytes": "0",
                "provider": "edge-tts",
                "voice": body.voice,
                "rate": body.rate,
                "volume": body.volume,
                "pitch": body.pitch,
                "skip_reason": "unspeakable_punctuation_only",
                "text_length": str(len(text)),
                "updated_at": str(int(time.time())),
                "request_id": request_id,
                "trace_id": trace_id,
            }
            await _write_segment_meta(client, body.cache_key, meta)
            return _segment_response_from_meta(body.cache_key, meta)

        async def _on_early_failed(attempt: int, last_error: Exception | None) -> None:
            """Callback fired once when hedge_after_attempts is reached.

            Writes short-TTL early_failed meta to Redis so the orchestrator
            can re-dispatch immediately, and fires an early_failed webhook.
            The edge worker continues retrying after this callback returns.
            """
            ef_meta = {
                "status": "early_failed",
                "cache_key": body.cache_key,
                "provider": "edge-tts",
                "last_error": str(last_error)[:500] if last_error else "early_failed",
                "attempts": str(attempt),
                "failed_text_length": str(len(text)),
                "failed_text_preview": _failed_text_preview(text),
                "updated_at": str(int(time.time())),
                "request_id": request_id,
                "trace_id": trace_id,
            }
            pipe = client.pipeline()
            pipe.hset(_segment_meta_key(body.cache_key), mapping=ef_meta)
            pipe.expire(_segment_meta_key(body.cache_key), 60)
            await pipe.execute()
            log.warning(
                "edge_tts_segment_early_failed",
                request_id=request_id,
                trace_id=trace_id,
                cache_key=body.cache_key,
                voice=body.voice,
                attempts=attempt,
                text_length=len(text),
                error=str(last_error),
                elapsed_ms=round((time.perf_counter() - started_at) * 1000, 2),
            )
            early_response = TTSSegmentResponse(
                status="early_failed",
                cache_key=body.cache_key,
                attempts=attempt,
                error=str(last_error)[:500] if last_error else "early_failed",
            )
            _fire_webhook_background(body.webhook_url, early_response, request_id=request_id)

        audio, duration, duration_source, voice_id = await edge_tts_service.synthesize_text_bytes(
            text=text,
            voice=body.voice,
            rate=body.rate,
            volume=body.volume,
            pitch=body.pitch,
            priority_score=body.priority_score,
            time_to_play_ms=body.time_to_play_ms,
            trace_id=trace_id,
            on_early_failed=_on_early_failed,
        )

        # Write to local disk
        await asyncio.to_thread(media_storage.upload_bytes, audio, storage_key)
        url = media_storage.url(storage_key)

        meta = {
            "status": "ready",
            "cache_key": body.cache_key,
            "url": url,
            "duration": f"{float(duration or 0):.3f}",
            "duration_source": duration_source,
            "bytes": str(len(audio)),
            "provider": "edge-tts",
            "voice": voice_id,
            "rate": body.rate,
            "volume": body.volume,
            "pitch": body.pitch,
            "attempts": str(edge_tts_service.retries),
            "updated_at": str(int(time.time())),
            "request_id": request_id,
            "trace_id": trace_id,
        }
        await _write_segment_meta(client, body.cache_key, meta)
        return _segment_response_from_meta(body.cache_key, meta)


    except Exception as exc:
        meta = {
            "status": "failed",
            "cache_key": body.cache_key,
            "provider": "edge-tts",
            "last_error": str(exc)[:500],
            "failed_text_length": str(len(text)),
            "failed_text_preview": _failed_text_preview(text),
            "updated_at": str(int(time.time())),
            "request_id": request_id,
            "trace_id": trace_id,
        }
        await _write_segment_meta(client, body.cache_key, meta)
        log.warning(
            "edge_tts_segment_synth_failed",
            request_id=request_id,
            trace_id=trace_id,
            cache_key=body.cache_key,
            voice=body.voice,
            text_length=len(text),
            error=str(exc),
            elapsed_ms=round((time.perf_counter() - started_at) * 1000, 2),
        )
        raise


async def _synthesize_segment_background(
    *,
    body: TTSSegmentRequest,
    client: redis.Redis,
    lock_key: str,
    lock_token: str,
    request_id: str,
    trace_id: str,
    text: str,
    storage_key: str,
    started_at: float,
) -> None:
    response: TTSSegmentResponse | None = None
    try:
        response = await _synthesize_segment_to_meta(
            body=body,
            client=client,
            request_id=request_id,
            trace_id=trace_id,
            text=text,
            storage_key=storage_key,
            started_at=started_at,
        )
    except Exception:
        # Build a failed response for the webhook
        cached = await client.hgetall(_segment_meta_key(body.cache_key))
        if cached:
            response = _segment_response_from_meta(body.cache_key, cached)
    finally:
        await _release_segment_lock(client, lock_key, lock_token)
        if response and body.webhook_url:
            _fire_webhook_background(body.webhook_url, response, request_id=request_id)


# ──────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    redis_ok = True
    redis_error = None
    try:
        client = await _segment_redis()
        await client.ping()
    except Exception as exc:
        redis_ok = False
        redis_error = str(exc)

    metrics = edge_tts_service.get_metrics(include_cache=False)
    return {
        "status": "ok" if redis_ok else "unhealthy",
        "service": "remote-edge-tts",
        "checks": {
            "redis": {"ok": redis_ok, "error": redis_error},
            "storage": {"ok": True, "type": "local", "root_dir": LOCAL_DIR},
            "synthesis": {
                "ok": True,
                "queue_size": metrics.get("queue_size", 0),
                "active_jobs": metrics.get("active_jobs", 0),
                "worker_count": metrics.get("worker_count", 0),
            },
        },
    }


@app.get("/api/tts/status")
async def tts_status(_: None = Depends(require_internal_token)):
    metrics = edge_tts_service.get_metrics(include_cache=False)
    health = metrics.get("health", {})
    status = health.get("status", "healthy")
    return {
        "status": status,
        "service": "remote-edge-tts",
        "uptime_seconds": health.get("uptime_seconds"),
        "public_base_url": PUBLIC_BASE_URL,
        "cleanup": {
            "max_age_seconds": CLEANUP_MAX_AGE_SECONDS,
            "interval_seconds": CLEANUP_INTERVAL_SECONDS,
        },
        "checks": {
            "redis": {"ok": bool(REDIS_URL)},
            "storage": {"ok": True, "type": "local", "root_dir": LOCAL_DIR},
            "synthesis": {
                "ok": status in {"excellent", "good", "healthy", "degraded"},
                "success_rate": health.get("success_rate"),
                "synth_failures": metrics.get("synth_failures", 0),
                "failed_jobs": metrics.get("failed_jobs", 0),
            },
        },
        "metrics": metrics,
    }


@app.get("/api/tts/voices")
async def list_tts_voices():
    return {
        "voices": [
            {"alias": "hoaimy", "voice": "vi-VN-HoaiMyNeural", "gender": "female"},
            {"alias": "namminh", "voice": "vi-VN-NamMinhNeural", "gender": "male"},
        ],
        "aliases": VOICE_ALIASES,
    }


@app.get("/api/tts/metrics")
async def metrics(_: None = Depends(require_internal_token)):
    return _metrics_payload()


@app.post("/api/tts/segments", response_model=TTSSegmentResponse)
async def create_tts_segment(
    body: TTSSegmentRequest,
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    _: None = Depends(require_internal_token),
):
    request_id = _request_id_header(x_request_id or body.trace.request_id)
    #log.info("received_tts_segment_request", request_id=request_id, cache_key=body.cache_key)
    trace = body.trace
    trace_id = _trace_id(request_id, trace.chain_id or None, trace.segment_index)
    text = _preclean_tts_text(body.text)

    if len(text) > MAX_SEGMENT_CHARS:
        raise HTTPException(status_code=413, detail="text exceeds EDGE_TTS_MAX_SEGMENT_CHARS")

    try:
        edge_tts_service.normalize_voice(body.voice)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    storage_key = _segment_storage_key(body.cache_key)
    client = await _segment_redis()

    # Check Redis cache first
    cached = await client.hgetall(_segment_meta_key(body.cache_key))
    if cached.get("status") == "ready" and cached.get("url"):
        response = _segment_response_from_meta(body.cache_key, cached)
        _fire_webhook_background(body.webhook_url, response, request_id=request_id)
        return response

    # Try to acquire lock
    lock_key = _segment_lock_key(body.cache_key)
    lock_token = f"{request_id}:{time.time_ns()}"
    lock_acquired = await client.set(lock_key, lock_token, ex=SEGMENT_LOCK_SECONDS, nx=True)

    if not lock_acquired:
        cached = await client.hgetall(_segment_meta_key(body.cache_key))
        if cached:
            response = _segment_response_from_meta(body.cache_key, cached)
            if response.status in ("ready", "failed", "early_failed"):
                _fire_webhook_background(body.webhook_url, response, request_id=request_id)
            return response
        return _generating_response(body.cache_key, request_id)

    started_at = time.perf_counter()
    lock_released_by_background = False

    try:
        # Double-check cache after acquiring lock
        cached = await client.hgetall(_segment_meta_key(body.cache_key))
        if cached.get("status") == "ready" and cached.get("url"):
            response = _segment_response_from_meta(body.cache_key, cached)
            _fire_webhook_background(body.webhook_url, response, request_id=request_id)
            return response

        # Always run in background (fire-and-forget)
        task = asyncio.create_task(
            _synthesize_segment_background(
                body=body,
                client=client,
                lock_key=lock_key,
                lock_token=lock_token,
                request_id=request_id,
                trace_id=trace_id,
                text=text,
                storage_key=storage_key,
                started_at=started_at,
            )
        )
        _track_background_task(task)
        lock_released_by_background = True
        return _generating_response(body.cache_key, request_id)

    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        # Fire failed webhook
        failed_response = TTSSegmentResponse(
            status="failed",
            cache_key=body.cache_key,
            error=str(exc)[:500],
        )
        _fire_webhook_background(body.webhook_url, failed_response, request_id=request_id)
        raise HTTPException(status_code=503, detail=f"Edge TTS segment failed: {exc}") from exc
    finally:
        if not lock_released_by_background:
            await _release_segment_lock(client, lock_key, lock_token)


# ──────────────────────────────────────────────────────────────────────
# Metrics
# ──────────────────────────────────────────────────────────────────────

def _metrics_payload() -> dict:
    now = time.monotonic()
    if METRICS_CACHE_SECONDS > 0 and metrics_cache["payload"] and now < metrics_cache["expires_at"]:
        return metrics_cache["payload"]

    payload = edge_tts_service.get_metrics(include_cache=False)
    payload["storage"] = {
        "type": "local",
        "root_dir": LOCAL_DIR,
        "public_base_url": PUBLIC_BASE_URL,
    }
    payload["redis"] = {
        "enabled": bool(REDIS_URL),
        "segment_meta_ttl_seconds": SEGMENT_META_TTL_SECONDS,
        "segment_lock_seconds": SEGMENT_LOCK_SECONDS,
    }
    payload["cleanup"] = {
        "max_age_seconds": CLEANUP_MAX_AGE_SECONDS,
        "interval_seconds": CLEANUP_INTERVAL_SECONDS,
    }
    if METRICS_CACHE_SECONDS > 0:
        metrics_cache["payload"] = payload
        metrics_cache["expires_at"] = now + METRICS_CACHE_SECONDS
    return payload
