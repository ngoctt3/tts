import datetime
import structlog
import os
import logging

from colorama import Fore, Style

LOG_FILE = os.getenv("LOG_FILE", "logs/log.txt")

def add_prefix_processor(_, __, event_dict):
    prefix = event_dict.pop("_prefix", None)
    if prefix:
        event_dict["event"] = f"{prefix} {event_dict['event']}"
    return event_dict

class CustomBoundLogger(structlog.stdlib.BoundLogger):
    def set_prefix(self, prefix: str):
        return self.bind(_prefix=prefix)

    def success(self, event, *args, **kw):
        return self.info(f"✅ {event}", *args, **kw)

_setup_done = False

def setup_logger():
    global _setup_done
    if _setup_done:
        return
    
    import sys
    
    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(LOG_FILE)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Ensure stdout/stderr use UTF-8 on Windows
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding='utf-8')
            sys.stderr.reconfigure(encoding='utf-8')
        except (AttributeError, Exception):
            pass

    # Common processors for both console and file
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S.%f", utc=False),
        add_prefix_processor,
    ]

    # Console Handler (with colors)
    console_handler = logging.StreamHandler(sys.stdout)
    console_formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.dev.ConsoleRenderer(colors=True),
        foreign_pre_chain=processors,
    )
    console_handler.setFormatter(console_formatter)

    # File Handler (no colors, clean text for log.txt)
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.dev.ConsoleRenderer(colors=False),
        foreign_pre_chain=processors,
    )
    file_handler.setFormatter(file_formatter)

    # Get log level from settings or env
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    try:
        log_level = getattr(logging, log_level_str)
    except AttributeError:
        log_level = logging.INFO

    logging.basicConfig(
        level=log_level,
        handlers=[console_handler, file_handler]
    )

    # Silence 3rd party loggers
    logging.getLogger("httpx").setLevel(logging.ERROR)
    logging.getLogger("httpcore").setLevel(logging.ERROR)
    logging.getLogger("uvicorn.access").setLevel(logging.ERROR)
    logging.getLogger("asyncpg").setLevel(logging.WARNING)
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("google_genai").setLevel(logging.WARNING)

    structlog.configure(
        processors=processors + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=CustomBoundLogger,
        cache_logger_on_first_use=True,
    )
    _setup_done = True

# Initialize on import so loggers created at module level have success()
setup_logger()

def get_logger(name='bbot'):
    """Get a configured logger instance."""
    return structlog.get_logger(name)
