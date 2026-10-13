"""Structured JSON logging.

One JSON object per line, so the output stays greppable in a terminal and
parses in Airflow's task logs without a custom pattern.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import UTC, datetime

_RESERVED = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {
    "message",
    "asctime",
    "taskName",
}

# Never emit these keys, whatever a caller passes in extra={...}.
_REDACT = {"password", "passwd", "secret", "token", "authorization", "api_key"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "severity": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in _RESERVED or key.startswith("_"):
                continue
            payload[key] = "***" if key.lower() in _REDACT else value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str | None = None) -> None:
    resolved = (level or os.environ.get("LOG_LEVEL") or "INFO").upper()
    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(resolved)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
