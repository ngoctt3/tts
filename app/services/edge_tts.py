import asyncio
import html
import math
import os
import random
import re
import sys
import time
from collections import deque
from typing import Any, List, Optional

from bs4 import BeautifulSoup

from app.core.logger import get_logger


if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


DEFAULT_TTS_CONCURRENCY = int(os.getenv("EDGE_TTS_CONCURRENCY", "32"))
DEFAULT_TTS_RETRIES = int(os.getenv("EDGE_TTS_RETRIES", "10"))
DEFAULT_HEDGE_AFTER_ATTEMPTS = int(os.getenv("EDGE_TTS_HEDGE_AFTER_ATTEMPTS", "4"))
DEFAULT_CONNECT_TIMEOUT = int(os.getenv("EDGE_TTS_CONNECT_TIMEOUT", "8"))
DEFAULT_RECEIVE_TIMEOUT = int(os.getenv("EDGE_TTS_RECEIVE_TIMEOUT", "30"))
DEFAULT_SHORT_SEGMENT_CHARS = 180
DEFAULT_LONG_SEGMENT_CHARS = 300
DEFAULT_SHORT_SEGMENT_WORDS = 30
DEFAULT_LONG_SEGMENT_WORDS = 50
DEFAULT_SHORT_SEGMENT_COUNT = 5
DEFAULT_PROXY_URLS = [item.strip() for item in os.getenv("EDGE_TTS_PROXY_URLS", "").split(",") if item.strip()]
RETRY_SLEEP_MIN_SECONDS = float(os.getenv("EDGE_TTS_RETRY_SLEEP_MIN_SECONDS", "0.1"))
RETRY_SLEEP_MAX_SECONDS = float(os.getenv("EDGE_TTS_RETRY_SLEEP_MAX_SECONDS", "0.2"))
METRICS_SAMPLE_SIZE = int(os.getenv("EDGE_TTS_METRICS_SAMPLE_SIZE", "1000"))
HARD_SPLIT_LOOKAHEAD_CHARS = 80

SENTENCE_END_RE = re.compile(r"(?<=[.!?])\s+|(?<=[\u3002\uff01\uff1f])\s*")

VOICE_ALIASES = {
    "hoaimy": "vi-VN-HoaiMyNeural",
    "female": "vi-VN-HoaiMyNeural",
    "namminh": "vi-VN-NamMinhNeural",
    "male": "vi-VN-NamMinhNeural",
    "aria": "en-US-AriaNeural",
    "english": "en-US-AriaNeural",
    "en-female": "en-US-AriaNeural",
    "guy": "en-US-GuyNeural",
    "en-male": "en-US-GuyNeural",
    "emma-ml": "en-US-EmmaMultilingualNeural",
    "jenny-ml": "en-US-JennyMultilingualNeural",
    "serena-ml": "en-US-SerenaMultilingualNeural",
    "andrew-ml": "en-US-AndrewMultilingualNeural",
    "brian-ml": "en-US-BrianMultilingualNeural",
    "ryan-ml": "en-US-RyanMultilingualNeural",
    "steffan-ml": "en-US-SteffanMultilingualNeural",
}

SUPPORTED_VOICES = {
    "vi-VN-HoaiMyNeural",
    "vi-VN-NamMinhNeural",
    "en-US-AriaNeural",
    "en-US-GuyNeural",
    "en-US-EmmaMultilingualNeural",
    "en-US-JennyMultilingualNeural",
    "en-US-SerenaMultilingualNeural",
    "en-US-AndrewMultilingualNeural",
    "en-US-BrianMultilingualNeural",
    "en-US-RyanMultilingualNeural",
    "en-US-SteffanMultilingualNeural",
}


class EdgeTTSService:
    """Stateless Edge TTS engine.

    This class intentionally does not know sessions, chapters, manifests, m3u8,
    local MP3 files, or cache directories. It only provides text splitting for
    the API planner and text-to-MP3 bytes synthesis for the edge service.
    """

    def __init__(
        self,
        *,
        concurrency: int = DEFAULT_TTS_CONCURRENCY,
        retries: int = DEFAULT_TTS_RETRIES,
        hedge_after_attempts: int = DEFAULT_HEDGE_AFTER_ATTEMPTS,
        proxy_urls: Optional[List[str]] = None,
        short_segment_chars: int = DEFAULT_SHORT_SEGMENT_CHARS,
        long_segment_chars: int = DEFAULT_LONG_SEGMENT_CHARS,
        short_segment_words: int = DEFAULT_SHORT_SEGMENT_WORDS,
        long_segment_words: int = DEFAULT_LONG_SEGMENT_WORDS,
        short_segment_count: int = DEFAULT_SHORT_SEGMENT_COUNT,
    ):
        self.concurrency = max(1, int(concurrency))
        self.retries = max(1, int(retries))
        self.hedge_after_attempts = max(0, int(hedge_after_attempts))
        self.proxy_urls = list(proxy_urls if proxy_urls is not None else DEFAULT_PROXY_URLS)
        self.short_segment_chars = max(1, int(short_segment_chars))
        self.long_segment_chars = max(1, int(long_segment_chars))
        self.short_segment_words = max(0, int(short_segment_words))
        self.long_segment_words = max(0, int(long_segment_words))
        self.short_segment_count = max(0, int(short_segment_count))

        self.log = get_logger().bind(service="edge_tts_engine")
        self._queue: asyncio.PriorityQueue | None = None
        self._worker_loop: asyncio.AbstractEventLoop | None = None
        self._worker_tasks: list[asyncio.Task] = []
        self._queue_sequence = 0
        self._proxy_cursor = 0
        self._service_started_at = time.time()
        self._active_jobs = 0
        self._completed_jobs = 0
        self._failed_jobs = 0
        self._completed_words = 0
        self._completed_chars = 0
        self._completed_bytes = 0
        self._completed_audio_seconds = 0.0
        self._synth_attempts = 0
        self._synth_failures = 0
        self._lane_attempts: dict[str, int] = {}
        self._lane_failures: dict[str, int] = {}
        self._lane_successes: dict[str, int] = {}
        self._queue_wait_ms = deque(maxlen=METRICS_SAMPLE_SIZE)
        self._synth_ms = deque(maxlen=METRICS_SAMPLE_SIZE)
        self._duration_ms = deque(maxlen=METRICS_SAMPLE_SIZE)
        self._completion_events = deque(maxlen=METRICS_SAMPLE_SIZE)
        self._stage_metrics: dict[str, dict] = {}

    def normalize_voice(self, voice: str) -> str:
        voice_id = VOICE_ALIASES.get((voice or "").lower(), voice)
        if voice_id not in SUPPORTED_VOICES:
            raise ValueError(f"Unsupported voice: {voice}")
        return voice_id

    def split_html(self, content_html: str, segment_index_offset: int = 0) -> list[str]:
        blocks = self._html_text_blocks(content_html)
        return self._split_blocks_with_tiers(blocks, segment_index_offset=segment_index_offset)

    def split_html_with_spans(self, content_html: str, segment_index_offset: int = 0) -> list[dict[str, Any]]:
        plain_text = self.plain_text_from_html(content_html)
        chunks = self.split_html(content_html, segment_index_offset=segment_index_offset)
        cursor = 0
        segments = []
        for chunk in chunks:
            start = plain_text.find(chunk, cursor)
            if start < 0:
                start = max(0, min(cursor, len(plain_text)))
            end = min(len(plain_text), start + len(chunk))
            segments.append({"text": chunk, "plain_start": start, "plain_end": end})
            cursor = end
        return segments

    def plain_text_from_html(self, content_html: str) -> str:
        return " ".join(self._html_text_blocks(content_html))

    def _html_text_blocks(self, content_html: str) -> list[str]:
        soup = BeautifulSoup(content_html or "", "html.parser")
        blocks = []
        for tag in soup.find_all(["p", "div", "li", "h1", "h2", "h3", "blockquote"]):
            text_value = self._normalize_text(tag.get_text(" ", strip=True))
            if text_value:
                blocks.append(text_value)
        if not blocks:
            fallback = self._normalize_text(soup.get_text(" ", strip=True) if soup else content_html)
            if fallback:
                blocks.append(fallback)
        return blocks

    async def synthesize_text_bytes(
        self,
        *,
        text: str,
        voice: str = "hoaimy",
        rate: str = "+0%",
        volume: str = "+0%",
        pitch: str = "+0Hz",
        priority_score: float,
        time_to_play_ms: float,
        trace_id: str | None = None,
    ) -> tuple[bytes, float, str, str]:
        await self._ensure_workers()
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._queue_sequence += 1
        queued_at = time.perf_counter()
        priority = float(priority_score)
        await self._queue.put(
            (
                priority,
                self._queue_sequence,
                {
                    "future": future,
                    "queued_at": queued_at,
                    "text": text,
                    "voice": voice,
                    "rate": rate,
                    "volume": volume,
                    "pitch": pitch,
                    "priority_score": priority,
                    "time_to_play_ms": time_to_play_ms,
                    "trace_id": trace_id,
                },
            )
        )
        return await future

    async def _synthesize_text_bytes_now(
        self,
        *,
        text: str,
        voice: str = "hoaimy",
        rate: str = "+0%",
        volume: str = "+0%",
        pitch: str = "+0Hz",
    ) -> tuple[bytes, float, str, str]:
        voice_id = self.normalize_voice(voice)
        started_at = time.perf_counter()
        self._active_jobs += 1
        try:
            audio = await self._synthesize_with_retries(
                text=text,
                voice=voice_id,
                rate=rate,
                volume=volume,
                pitch=pitch,
            )
            duration_started = time.perf_counter()
            duration = self._mp3_duration_seconds(audio)
            self._duration_ms.append((time.perf_counter() - duration_started) * 1000)
            words = len((text or "").split())
            self._completed_jobs += 1
            self._completed_words += words
            self._completed_chars += len(text or "")
            self._completed_bytes += len(audio)
            self._completed_audio_seconds += float(duration or 0)
            self._completion_events.append((time.time(), words, len(text or ""), len(audio), float(duration or 0)))
            return audio, round(duration, 3), "mp3_scan", voice_id
        except Exception:
            self._failed_jobs += 1
            raise
        finally:
            self._active_jobs -= 1

    async def shutdown(self) -> None:
        tasks = list(self._worker_tasks)
        self._worker_tasks = []
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._queue = None
        self._worker_loop = None

    def get_metrics(self, *, include_cache: bool = False) -> dict:
        total_jobs = self._completed_jobs + self._failed_jobs
        success_rate = self._completed_jobs / total_jobs if total_jobs else 1.0
        queue_size = self._queue.qsize() if self._queue is not None else 0
        return {
            "mode": "stateless-segment",
            "worker_count": self.concurrency,
            "active_jobs": self._active_jobs,
            "queue_size": queue_size,
            "queue_max_size": 0,
            "completed_jobs": self._completed_jobs,
            "failed_jobs": self._failed_jobs,
            "completed_words": self._completed_words,
            "completed_chars": self._completed_chars,
            "completed_bytes": self._completed_bytes,
            "completed_audio_seconds": round(self._completed_audio_seconds, 3),
            "synth_attempts": self._synth_attempts,
            "synth_failures": self._synth_failures,
            "lane_attempts": dict(self._lane_attempts),
            "lane_failures": dict(self._lane_failures),
            "lane_successes": dict(self._lane_successes),
            "latency_ms": {
                "queue_wait": self._latency_summary(self._queue_wait_ms),
                "synthesize": self._latency_summary(self._synth_ms),
                "duration_scan": self._latency_summary(self._duration_ms),
            },
            "throughput": self._throughput_snapshot(),
            "stages": self._stage_metrics_snapshot(),
            "health": {
                "status": "healthy" if success_rate >= 0.8 else "degraded",
                "success_rate": round(success_rate, 4),
                "uptime_seconds": round(time.time() - self._service_started_at, 3),
            },
        }

    async def _ensure_workers(self) -> None:
        loop = asyncio.get_running_loop()
        alive = [task for task in self._worker_tasks if not task.done()]
        if self._queue is not None and self._worker_loop is loop and len(alive) == self.concurrency:
            self._worker_tasks = alive
            return
        for task in alive:
            task.cancel()
        if alive:
            await asyncio.gather(*alive, return_exceptions=True)
        self._queue = asyncio.PriorityQueue()
        self._worker_loop = loop
        self._worker_tasks = [asyncio.create_task(self._queue_worker(index), name=f"edge-tts-worker-{index}") for index in range(self.concurrency)]

    async def _queue_worker(self, worker_index: int) -> None:
        while True:
            _priority, _sequence, job = await self._queue.get()
            future = job["future"]
            try:
                self._queue_wait_ms.append((time.perf_counter() - job["queued_at"]) * 1000)
                result = await self._synthesize_text_bytes_now(
                    text=job["text"],
                    voice=job["voice"],
                    rate=job["rate"],
                    volume=job["volume"],
                    pitch=job["pitch"],
                )
                if not future.done():
                    future.set_result(result)
            except Exception as exc:
                if not future.done():
                    future.set_exception(exc)
            finally:
                self._queue.task_done()

    async def _synthesize_with_retries(self, *, text: str, voice: str, rate: str, volume: str, pitch: str) -> bytes:
        if self.proxy_urls and self.hedge_after_attempts == 0:
            return await self._synthesize_lane_retries(
                text=text,
                voice=voice,
                rate=rate,
                volume=volume,
                pitch=pitch,
                lane="proxy",
                proxy_url=self._next_proxy_url(),
                attempts=self.retries,
            )

        main_attempts = self.retries
        if self.proxy_urls and 0 < self.hedge_after_attempts < self.retries:
            main_attempts = self.hedge_after_attempts

        try:
            return await self._synthesize_lane_retries(
                text=text,
                voice=voice,
                rate=rate,
                volume=volume,
                pitch=pitch,
                lane="main",
                proxy_url=None,
                attempts=main_attempts,
            )
        except Exception:
            remaining = self.retries - main_attempts
            if not self.proxy_urls or remaining <= 0:
                raise
            return await self._synthesize_lane_retries(
                text=text,
                voice=voice,
                rate=rate,
                volume=volume,
                pitch=pitch,
                lane="proxy",
                proxy_url=self._next_proxy_url(),
                attempts=max(1, remaining),
            )

    async def _synthesize_lane_retries(
        self,
        *,
        text: str,
        voice: str,
        rate: str,
        volume: str,
        pitch: str,
        lane: str,
        proxy_url: str | None,
        attempts: int,
    ) -> bytes:
        last_error: Exception | None = None
        for attempt in range(1, max(1, attempts) + 1):
            self._synth_attempts += 1
            self._lane_attempts[lane] = self._lane_attempts.get(lane, 0) + 1
            try:
                audio = await self._synthesize_once(text, voice, rate, volume, pitch, proxy_url=proxy_url)
                if len(audio) < 512:
                    raise RuntimeError("Edge TTS returned an empty audio segment")
                self._lane_successes[lane] = self._lane_successes.get(lane, 0) + 1
                return audio
            except Exception as exc:
                last_error = exc
                self._synth_failures += 1
                self._lane_failures[lane] = self._lane_failures.get(lane, 0) + 1
                if attempt >= 7:
                    self.log.warning(
                        "edge_tts_synthesize_attempt_failed",
                        lane=lane,
                        attempt=attempt,
                        max_attempts=attempts,
                        proxy_enabled=bool(proxy_url),
                        voice=voice,
                        text_length=len(text),
                        text_preview=text[:120],
                        error=str(exc),
                    )
                if attempt < attempts and RETRY_SLEEP_MAX_SECONDS > 0:
                    sleep_min = max(0.0, min(RETRY_SLEEP_MIN_SECONDS, RETRY_SLEEP_MAX_SECONDS))
                    sleep_max = max(sleep_min, RETRY_SLEEP_MAX_SECONDS)
                    await asyncio.sleep(random.uniform(sleep_min, sleep_max))
        raise RuntimeError(f"Edge TTS failed after {attempts} {lane} attempt(s): {last_error}")

    async def _synthesize_once(
        self,
        text: str,
        voice: str,
        rate: str,
        volume: str,
        pitch: str,
        *,
        proxy_url: str | None,
    ) -> bytes:
        import edge_tts

        started_at = time.perf_counter()
        communicate = edge_tts.Communicate(
            text=text,
            voice=voice,
            rate=rate,
            volume=volume,
            pitch=pitch,
            proxy=proxy_url,
            connect_timeout=DEFAULT_CONNECT_TIMEOUT,
            receive_timeout=DEFAULT_RECEIVE_TIMEOUT,
        )
        chunks: list[bytes] = []
        async for message in communicate.stream():
            if message["type"] == "audio":
                chunks.append(message["data"])
        audio = b"".join(chunks)
        self._synth_ms.append((time.perf_counter() - started_at) * 1000)
        return audio

    async def _to_thread_stage(self, stage: str, func, *args, **kwargs):
        def run():
            started_at = time.perf_counter()
            started_cpu = time.thread_time()
            try:
                return func(*args, **kwargs)
            finally:
                self._record_stage_metric(stage, started_at, started_cpu)

        return await asyncio.to_thread(run)

    def _next_proxy_url(self) -> str | None:
        if not self.proxy_urls:
            return None
        proxy_url = self.proxy_urls[self._proxy_cursor % len(self.proxy_urls)]
        self._proxy_cursor += 1
        return proxy_url

    def _split_blocks_with_tiers(self, blocks: list[str], segment_index_offset: int = 0) -> list[str]:
        segments: list[str] = []
        pending = list(blocks)
        offset = max(0, int(segment_index_offset or 0))
        while pending:
            absolute_index = offset + len(segments)
            char_limit = self._segment_char_limit(absolute_index)
            word_limit = self._segment_word_limit(absolute_index)
            block = pending.pop(0)
            parts = self._split_text_for_limit(block, char_limit, word_limit)
            if not parts:
                continue
            segments.append(parts[0])
            pending = parts[1:] + pending
        return segments

    def _split_text_for_limit(self, text_value: str, char_limit: int, word_limit: int = 0) -> list[str]:
        text_value = self._normalize_text(text_value)
        if not text_value:
            return []
        if len(text_value) <= char_limit and (word_limit <= 0 or len(text_value.split()) <= word_limit):
            return [text_value]

        sentences = self._split_sentences(text_value)
        if len(sentences) <= 1:
            return self._split_by_words(text_value, char_limit)

        segments: list[str] = []
        current: list[str] = []
        current_words = 0
        for sentence in sentences:
            sentence_words = len(sentence.split())
            candidate = " ".join(current + [sentence]).strip()
            over_chars = len(candidate) > char_limit
            over_words = word_limit > 0 and current_words + sentence_words > word_limit
            if current and (over_chars or over_words):
                segments.append(" ".join(current).strip())
                current = [sentence]
                current_words = sentence_words
            else:
                current.append(sentence)
                current_words += sentence_words
        if current:
            segments.append(" ".join(current).strip())
        flattened: list[str] = []
        for segment in segments:
            if len(segment) > char_limit * 2:
                flattened.extend(self._split_by_words(segment, char_limit))
            else:
                flattened.append(segment)
        return flattened

    def _segment_char_limit(self, absolute_index: int) -> int:
        return self.short_segment_chars if absolute_index < self.short_segment_count else self.long_segment_chars

    def _segment_word_limit(self, absolute_index: int) -> int:
        return self.short_segment_words if absolute_index < self.short_segment_count else self.long_segment_words

    def _record_stage_metric(self, stage: str, started_at: float, started_cpu: float) -> None:
        wall_ms = (time.perf_counter() - started_at) * 1000
        cpu_ms = max(0.0, (time.thread_time() - started_cpu) * 1000)
        bucket = self._stage_metrics.setdefault(
            stage,
            {
                "wall_ms": deque(maxlen=METRICS_SAMPLE_SIZE),
                "cpu_ms": deque(maxlen=METRICS_SAMPLE_SIZE),
                "wall_ms_total": 0.0,
                "cpu_ms_total": 0.0,
                "count": 0,
            },
        )
        bucket["wall_ms"].append(wall_ms)
        bucket["cpu_ms"].append(cpu_ms)
        bucket["wall_ms_total"] += wall_ms
        bucket["cpu_ms_total"] += cpu_ms
        bucket["count"] += 1

    def _stage_metrics_snapshot(self) -> dict:
        return {
            stage: {
                "count": data["count"],
                "wall_ms": self._latency_summary(data["wall_ms"]),
                "cpu_ms": self._latency_summary(data["cpu_ms"]),
                "wall_seconds_total": round(data["wall_ms_total"] / 1000, 3),
                "cpu_seconds_total": round(data["cpu_ms_total"] / 1000, 3),
            }
            for stage, data in sorted(self._stage_metrics.items())
        }

    def _throughput_snapshot(self) -> dict:
        uptime = max(0.001, time.time() - self._service_started_at)
        recent_cutoff = time.time() - 60
        recent = [event for event in self._completion_events if event[0] >= recent_cutoff]
        recent_span = max(1.0, min(60.0, uptime))
        return {
            "lifetime": {
                "segments_per_second": round(self._completed_jobs / uptime, 4),
                "words_per_second": round(self._completed_words / uptime, 4),
                "chars_per_second": round(self._completed_chars / uptime, 4),
                "audio_seconds_per_second": round(self._completed_audio_seconds / uptime, 4),
            },
            "recent_60s": {
                "segments": len(recent),
                "segments_per_second": round(len(recent) / recent_span, 4),
                "words_per_second": round(sum(event[1] for event in recent) / recent_span, 4),
                "chars_per_second": round(sum(event[2] for event in recent) / recent_span, 4),
                "bytes_per_second": round(sum(event[3] for event in recent) / recent_span, 4),
                "audio_seconds_per_second": round(sum(event[4] for event in recent) / recent_span, 4),
            },
        }

    @staticmethod
    def _split_sentences(text_value: str) -> list[str]:
        return [item.strip() for item in SENTENCE_END_RE.split(text_value) if item.strip()]

    @staticmethod
    def _normalize_text(value: str) -> str:
        return re.sub(r"\s+", " ", html.unescape(value or "")).strip()

    @staticmethod
    def _split_by_words(text_value: str, limit: int) -> list[str]:
        segments: list[str] = []
        remaining = text_value.strip()
        while len(remaining) > limit:
            split_at = EdgeTTSService._find_word_split(remaining, limit)
            if split_at >= len(remaining):
                segments.append(remaining)
                return segments
            segments.append(remaining[:split_at].strip())
            remaining = remaining[split_at:].strip()
        if remaining:
            segments.append(remaining)
        return segments

    @staticmethod
    def _find_word_split(text_value: str, limit: int) -> int:
        lookahead_limit = min(len(text_value), limit + HARD_SPLIT_LOOKAHEAD_CHARS)
        for index in range(lookahead_limit - 1, max(0, limit // 2), -1):
            if text_value[index] == " " and text_value[index - 1] in ".!?\u3002\uff01\uff1f":
                return index
        return len(text_value)

    @staticmethod
    def _estimate_duration(text_value: str) -> float:
        words = max(1, len((text_value or "").split()))
        return max(2.0, words / 165 * 60)

    @staticmethod
    def _latency_summary(values) -> dict:
        if not values:
            return {"count": 0}
        ordered = sorted(values)
        return {
            "count": len(ordered),
            "p50": round(EdgeTTSService._percentile(ordered, 0.50), 2),
            "p95": round(EdgeTTSService._percentile(ordered, 0.95), 2),
            "max": round(ordered[-1], 2),
            "avg": round(sum(ordered) / len(ordered), 2),
        }

    @staticmethod
    def _percentile(ordered: list[float], percentile: float) -> float:
        if len(ordered) == 1:
            return ordered[0]
        index = (len(ordered) - 1) * percentile
        lower = math.floor(index)
        upper = math.ceil(index)
        if lower == upper:
            return ordered[int(index)]
        weight = index - lower
        return ordered[lower] * (1 - weight) + ordered[upper] * weight

    @staticmethod
    def _mp3_duration_seconds(audio: bytes) -> float:
        if not audio:
            return 0.0

        index = 0
        if len(audio) >= 10 and audio[:3] == b"ID3":
            tag_size = (
                ((audio[6] & 0x7F) << 21)
                | ((audio[7] & 0x7F) << 14)
                | ((audio[8] & 0x7F) << 7)
                | (audio[9] & 0x7F)
            )
            index = 10 + tag_size

        bitrates = {
            3: {
                3: [0, 32, 64, 96, 128, 160, 192, 224, 256, 288, 320, 352, 384, 416, 448, 0],
                2: [0, 32, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384, 0],
                1: [0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 0],
            },
            2: {
                3: [0, 32, 48, 56, 64, 80, 96, 112, 128, 144, 160, 176, 192, 224, 256, 0],
                2: [0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160, 0],
                1: [0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160, 0],
            },
            0: {
                3: [0, 32, 48, 56, 64, 80, 96, 112, 128, 144, 160, 176, 192, 224, 256, 0],
                2: [0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160, 0],
                1: [0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160, 0],
            },
        }
        sample_rates = {
            3: [44100, 48000, 32000, 0],
            2: [22050, 24000, 16000, 0],
            0: [11025, 12000, 8000, 0],
        }
        samples_per_frame = {
            3: {3: 384, 2: 1152, 1: 1152},
            2: {3: 384, 2: 1152, 1: 576},
            0: {3: 384, 2: 1152, 1: 576},
        }

        total_seconds = 0.0
        frames = 0
        while index + 4 <= len(audio):
            if audio[index] != 0xFF or (audio[index + 1] & 0xE0) != 0xE0:
                index += 1
                continue
            version_bits = (audio[index + 1] >> 3) & 0x03
            layer_bits = (audio[index + 1] >> 1) & 0x03
            bitrate_index = (audio[index + 2] >> 4) & 0x0F
            sample_rate_index = (audio[index + 2] >> 2) & 0x03
            padding = (audio[index + 2] >> 1) & 0x01
            if version_bits == 1 or layer_bits == 0:
                index += 1
                continue
            bitrate = bitrates.get(version_bits, {}).get(layer_bits, [0] * 16)[bitrate_index] * 1000
            sample_rate = sample_rates.get(version_bits, [0] * 4)[sample_rate_index]
            samples = samples_per_frame.get(version_bits, {}).get(layer_bits, 0)
            if not bitrate or not sample_rate or not samples:
                index += 1
                continue
            if layer_bits == 3:
                frame_size = int(((12 * bitrate / sample_rate) + padding) * 4)
            else:
                coeff = 144 if version_bits == 3 else 72
                frame_size = int((coeff * bitrate / sample_rate) + padding)
            if frame_size <= 0:
                index += 1
                continue
            total_seconds += samples / sample_rate
            frames += 1
            index += frame_size

        if frames:
            return total_seconds
        return 0.0


edge_tts_service = EdgeTTSService()
