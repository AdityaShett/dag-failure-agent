"""
confidence.py -- signal extraction and weighted confidence scoring.

This module is the single source of truth for how a run's confidence score
is produced. It is deliberately dependency-free (stdlib only) so that:

  * the Cloud Run worker (nodes.py) can import it,
  * the offline benchmark runner (run_benchmark_local.py) can import it,
  * the weight tuner (tune_weights.py) can import it,

and all three produce *identical* numbers. Before this existed, the worker
computed a score from two degenerate signals (len(logs) > 50 and
len(source) > 50), which meant every run scored 1.0 and the weights in
config/weights.json had no effect on anything.

Signals
-------
Positive signals (0.0 - 1.0, higher = more confident a code fix is right):

  stack_trace_present         Is there a real traceback with an exception?
  line_number_matches_source  Does the failing frame actually exist in the
                              source we fetched? This is the guard against
                              analysing a log against a stale/wrong file.
  known_fix_pattern_match     Is the exception type one we have a safe,
                              mechanical fix pattern for?
  log_completeness            Timestamps / levels / dag+task context present?
  history_merge_rate          (optional) share of past agent PRs for this
                              dag+task that a human merged.
  retrieval_support           (optional) how much similar prior knowledge
                              the RAG step found.

Penalty signals (0.0 - 1.0 "detected-ness", carry a negative weight):

  external_dependency_detected  The failure bottoms out in a third-party
                                network call / outage. No code change here
                                fixes it, so this pulls the score under the
                                threshold on purpose.

Scoring
-------
Positive signals are averaged as a weighted mean over the signals that are
actually available on this run (optional signals that are None are dropped
and the denominator renormalised, so a missing history signal can't quietly
drag the score toward 0.5). Penalties are then subtracted outright. The
result is clamped to [0, 1].

    score = clamp( sum(w_i * s_i) / sum(w_i)  +  sum(p_j * d_j),  0, 1 )
"""

from __future__ import annotations

import json
import os
import re
from typing import Dict, Optional

# --------------------------------------------------------------------------
# Weights config
# --------------------------------------------------------------------------

WEIGHTS_CONFIG_PATH = os.environ.get("CONFIDENCE_WEIGHTS_PATH", "config/weights.json")

POSITIVE_SIGNALS = (
    "stack_trace_present",
    "line_number_matches_source",
    "known_fix_pattern_match",
    "log_completeness",
    "history_merge_rate",
    "retrieval_support",
)

PENALTY_SIGNALS = ("external_dependency_detected",)

ALL_SIGNALS = POSITIVE_SIGNALS + PENALTY_SIGNALS

# Used when config/weights.json is missing or unreadable. Same numbers as the
# shipped config so a missing file degrades to "documented default", not to
# "silently different behaviour".
DEFAULT_WEIGHTS: Dict[str, object] = {
    "version": 2,
    "signal_weights": {
        "stack_trace_present": 0.24,
        "line_number_matches_source": 0.30,
        "known_fix_pattern_match": 0.24,
        "log_completeness": 0.10,
        "history_merge_rate": 0.07,
        "retrieval_support": 0.05,
    },
    "penalty_weights": {
        "external_dependency_detected": -0.40,
    },
    "confidence_threshold": 0.70,
    "tier_thresholds": {"high": 0.85, "medium": 0.60},
}


class WeightsError(ValueError):
    """Raised when a weights config is structurally unusable."""


def normalize_weights_config(data: dict) -> dict:
    """Accepts any weights.json shape this project has ever written and
    returns the canonical v2 shape.

    Handles three historical shapes:
      v2 (current)  {"signal_weights": {...}, "penalty_weights": {...}, ...}
      v1 3-signal   {"weights": {"history": .., "logs": .., "source": ..}}
      v1 flat       {"history": .., "logs": .., "source": ..}

    The v1 shapes are mapped onto the nearest v2 signals so an old file
    still produces a working (if coarse) scorer instead of silently falling
    back to defaults -- that silent fallback is exactly what made the old
    config/weights.json a no-op.
    """
    if not isinstance(data, dict):
        raise WeightsError("weights config must be a JSON object")

    if "signal_weights" in data:
        signal_weights = dict(data["signal_weights"])
        penalty_weights = dict(data.get("penalty_weights", {}))

        # Tolerate a penalty that was written into signal_weights with a
        # negative value (the original file did exactly this).
        for name in list(signal_weights):
            if name in PENALTY_SIGNALS or signal_weights[name] < 0:
                penalty_weights.setdefault(name, signal_weights.pop(name))

        legacy = {"history": "history_merge_rate", "logs": "log_completeness",
                  "source": "line_number_matches_source"}
        for old, new in legacy.items():
            if old in signal_weights and new not in signal_weights:
                signal_weights[new] = signal_weights.pop(old)
    else:
        raw = data.get("weights", data)
        legacy = {"history": "history_merge_rate", "logs": "log_completeness",
                  "source": "line_number_matches_source"}
        signal_weights = {
            legacy[k]: float(v) for k, v in raw.items()
            if k in legacy and isinstance(v, (int, float))
        }
        penalty_weights = {}
        if not signal_weights:
            raise WeightsError(
                "no recognisable weights found (expected 'signal_weights', or "
                "legacy history/logs/source keys)"
            )

    signal_weights = {
        k: float(v) for k, v in signal_weights.items()
        if k in POSITIVE_SIGNALS and isinstance(v, (int, float))
    }
    if not signal_weights:
        raise WeightsError("signal_weights contained no known positive signals")
    if any(v < 0 for v in signal_weights.values()):
        raise WeightsError("positive signal weights must be >= 0")

    penalty_weights = {
        k: -abs(float(v)) for k, v in penalty_weights.items() if k in PENALTY_SIGNALS
    }

    threshold = float(data.get("confidence_threshold", 0.70))
    if not 0.0 <= threshold <= 1.0:
        raise WeightsError(f"confidence_threshold {threshold} outside [0, 1]")

    tiers = data.get("tier_thresholds") or {}
    high = float(tiers.get("high", max(0.85, threshold)))
    medium = float(tiers.get("medium", min(0.60, threshold)))

    out = {
        "version": 2,
        "signal_weights": signal_weights,
        "penalty_weights": penalty_weights,
        "confidence_threshold": threshold,
        "tier_thresholds": {"high": high, "medium": medium},
    }
    for passthrough in ("metadata", "notes", "_comment"):
        if passthrough in data:
            out[passthrough] = data[passthrough]
    return out


def load_weights(path: Optional[str] = None, strict: bool = False) -> dict:
    """Loads and validates the weights config.

    strict=True (used by tests and the tuner) raises on a broken file.
    strict=False (used by the worker at import time) logs and falls back to
    DEFAULT_WEIGHTS, but -- unlike the old loader -- it says loudly which
    path failed and why, and it does *not* treat a valid-but-legacy file as
    a failure.
    """
    path = path or WEIGHTS_CONFIG_PATH
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        cfg = normalize_weights_config(data)
        print(f"[confidence] loaded weights from {path}: "
              f"{cfg['signal_weights']} penalties={cfg['penalty_weights']} "
              f"threshold={cfg['confidence_threshold']}")
        return cfg
    except Exception as e:
        if strict:
            raise
        print(f"[confidence] WARNING: could not load weights from {path} ({e!r}); "
              f"falling back to built-in defaults")
        return normalize_weights_config(DEFAULT_WEIGHTS)


def save_weights(cfg: dict, path: str) -> None:
    cfg = normalize_weights_config(cfg)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)
        f.write("\n")


# --------------------------------------------------------------------------
# Log parsing
# --------------------------------------------------------------------------

_TRACEBACK_HEADER = re.compile(r"Traceback \(most recent call last\)", re.IGNORECASE)

# Matches both a clean traceback frame and the Airflow-style one where each
# line is prefixed with "[ts] ERROR - ".
_FRAME_RE = re.compile(
    r'File "(?P<file>[^"]+)", line (?P<line>\d+), in (?P<func>[^\s]+)'
)

# "module.QualifiedError: message" or bare "KeyError: 'x'"
_EXC_RE = re.compile(
    r"^(?:\[[^\]]*\]\s*)?(?:[A-Za-z_][\w.]*\s*-\s*)?"
    r"(?P<exc>(?:[a-zA-Z_][\w]*\.)*[A-Z][\w]*(?:Error|Exception|Timeout))"
    r"\s*:\s*(?P<msg>.*)$"
)

_LEVEL_RE = re.compile(r"\b(ERROR|CRITICAL|WARNING|INFO|DEBUG)\b")
_TIMESTAMP_RE = re.compile(r"\[\d{4}-\d{2}-\d{2}[ T,]")

# Exception types the agent has a small, mechanical, reviewable fix pattern
# for. Anything outside this set is not "no fix possible", it just doesn't
# get the pattern-match bonus.
KNOWN_FIX_PATTERNS = {
    "KeyError",
    "FileNotFoundError",
    "AttributeError",
    "ModuleNotFoundError",
    "ImportError",
    "NameError",
    "IndexError",
    "TypeError",
    "ValueError",
    "UnboundLocalError",
    "ZeroDivisionError",
}

# Libraries/markers that mean the failure came from something outside this
# repo's code: a third-party service, the network, or IAM.
_EXTERNAL_EXC_MARKERS = (
    "requests.exceptions",
    "httpx",
    "urllib3",
    "http.client",
    "socket.timeout",
    "ConnectionError",
    "ConnectTimeout",
    "ReadTimeout",
    "HTTPError",
    "SSLError",
    "google.api_core.exceptions",
    "Forbidden",
    "ServiceUnavailable",
    "TooManyRequests",
    "AirflowTaskTimeout",
)
_EXTERNAL_TEXT_MARKERS = (
    "500 server error",
    "502 bad gateway",
    "503 service unavailable",
    "504 gateway timeout",
    "429 too many requests",
    "403 ",
    "access denied",
    "internal server error",
    "max retries exceeded",
    "connection refused",
    "timed out",
    "execution_timeout",
    "status page",
    "partner-api",
    "external scoring service",
    "third-party",
)
_EXTERNAL_PATH_MARKERS = ("/site-packages/", "\\site-packages\\", "/dist-packages/")

# A frame inside *any* third-party package is not by itself evidence of an
# external dependency failure -- a KeyError raised inside pandas is still a
# bug in our DAG. Only frames inside a network/service client count.
_EXTERNAL_LIB_MARKERS = (
    "requests/", "requests\\", "httpx", "urllib3", "aiohttp",
    "google/api_core", "google\\api_core", "google/cloud", "google\\cloud",
    "botocore", "boto3", "http/client", "http\\client", "socket.py",
)


def _strip_log_prefix(line: str) -> str:
    """Airflow sometimes re-emits traceback lines through the logger, so a
    frame can arrive as '[2026-08-31 09:12:05] ERROR -   File "dag1.py"...'.
    Strip that wrapper so the same regexes work on both shapes."""
    line = re.sub(r"^\ufeff", "", line)
    line = re.sub(r"^\[[^\]]*\]\s*", "", line)
    line = re.sub(r"^\{[^}]*\}\s*", "", line)
    line = re.sub(r"^(ERROR|CRITICAL|WARNING|INFO|DEBUG)\s*-\s*", "", line)
    return line


def parse_log(task_logs: str) -> dict:
    """Extracts the structured facts the signals are computed from.

    Returns: has_traceback, frames (list of {file,line,func,code}),
    app_frame (the last frame that is *not* in site-packages -- i.e. the
    line in this repo that actually failed), exception_type,
    exception_message, has_timestamps, has_level, line_count.
    """
    text = task_logs or ""
    raw_lines = text.splitlines()
    lines = [_strip_log_prefix(l) for l in raw_lines]

    frames = []
    for i, line in enumerate(lines):
        m = _FRAME_RE.search(line)
        if not m:
            continue
        code = ""
        if i + 1 < len(lines):
            nxt = lines[i + 1].strip()
            if nxt and not _FRAME_RE.search(nxt) and not _EXC_RE.match(nxt):
                code = nxt
        frames.append({
            "file": m.group("file"),
            "line": int(m.group("line")),
            "func": m.group("func"),
            "code": code,
        })

    exception_type = None
    exception_message = ""
    for line in reversed(lines):
        stripped = line.strip()
        if not stripped:
            continue
        m = _EXC_RE.match(stripped)
        if m:
            exception_type = m.group("exc")
            exception_message = m.group("msg").strip()
            break

    app_frame = None
    for frame in reversed(frames):
        if not any(marker in frame["file"] for marker in _EXTERNAL_PATH_MARKERS):
            app_frame = frame
            break

    return {
        "has_traceback": bool(_TRACEBACK_HEADER.search(text)),
        "frames": frames,
        "app_frame": app_frame,
        "exception_type": exception_type,
        "exception_message": exception_message,
        "has_timestamps": bool(_TIMESTAMP_RE.search(text)),
        "has_level": bool(_LEVEL_RE.search(text)),
        "line_count": len([l for l in raw_lines if l.strip()]),
    }


def _short_exc(exception_type: Optional[str]) -> Optional[str]:
    if not exception_type:
        return None
    return exception_type.rsplit(".", 1)[-1]


# --------------------------------------------------------------------------
# Individual signals
# --------------------------------------------------------------------------

def signal_stack_trace_present(parsed: dict) -> float:
    if parsed["has_traceback"] and parsed["frames"] and parsed["exception_type"]:
        return 1.0
    if parsed["frames"] and parsed["exception_type"]:
        return 0.8
    if parsed["exception_type"]:
        return 0.4   # an exception line but no frames: we know what, not where
    return 0.0


def _normalize_code(code: str) -> str:
    return re.sub(r"\s+", "", code or "")


def signal_line_number_matches_source(parsed: dict, dag_source: str,
                                      tolerance: int = 3) -> float:
    """The mismatch guard.

    1.00 the failing line of code sits at (or within `tolerance` lines of)
         the line number the traceback claims -- source and log agree.
    0.60 that exact line of code exists in the file, but somewhere else --
         the file has drifted but the bug is still there.
    0.25 no code text to compare, but the claimed line number is at least
         inside the file and the failing function name exists in it.
    0.00 the line of code isn't in this file at all, or we have no source.
         This is the dag2-style "worker fetched the wrong/stale DAG" case
         that previously still scored 1.0 and went on to open a PR.
    """
    source = dag_source or ""
    if not source.strip():
        return 0.0

    frame = parsed.get("app_frame")
    if not frame:
        return 0.0

    src_lines = source.splitlines()
    n = len(src_lines)
    code = _normalize_code(frame.get("code", ""))

    if code:
        target = frame["line"]
        lo, hi = max(1, target - tolerance), min(n, target + tolerance)
        for idx in range(lo, hi + 1):
            if _normalize_code(src_lines[idx - 1]) == code:
                return 1.0
        for src_line in src_lines:
            if _normalize_code(src_line) == code:
                return 0.6
        # Last chance: a partial match (the logged frame can be truncated).
        if len(code) >= 12:
            for src_line in src_lines:
                if code in _normalize_code(src_line):
                    return 0.6
        return 0.0

    func = frame.get("func")
    in_range = 1 <= frame["line"] <= n
    func_present = bool(func) and (
        func == "<module>" or re.search(rf"\bdef\s+{re.escape(func)}\b", source)
    )
    if in_range and func_present:
        return 0.25
    return 0.0


def signal_known_fix_pattern_match(parsed: dict) -> float:
    exc = _short_exc(parsed.get("exception_type"))
    if not exc:
        return 0.0
    return 1.0 if exc in KNOWN_FIX_PATTERNS else 0.0


def signal_log_completeness(parsed: dict, task_logs: str,
                            dag_id: str = "", task_id: str = "") -> float:
    """Fraction of the things a reviewable log should have. Deliberately
    granular (not a 0/1 length check) so it can discriminate between a
    sparse one-liner and a full Airflow task log."""
    text = task_logs or ""
    checks = [
        parsed["has_timestamps"],
        parsed["has_level"],
        parsed["line_count"] >= 4,
        bool(task_id) and task_id in text,
        bool(dag_id) and dag_id in text,
        len(text) >= 200,
    ]
    return round(sum(1 for c in checks if c) / len(checks), 4)


def signal_external_dependency_detected(parsed: dict, task_logs: str) -> float:
    """Penalty signal. 1.0 = confidently an external/infra failure."""
    text = (task_logs or "").lower()
    exc = parsed.get("exception_type") or ""

    strong = any(marker.lower() in exc.lower() for marker in _EXTERNAL_EXC_MARKERS)

    frames = parsed.get("frames") or []
    bottoms_out_external = bool(frames) and any(
        marker in frames[-1]["file"] for marker in _EXTERNAL_LIB_MARKERS
    )

    text_hits = sum(1 for marker in _EXTERNAL_TEXT_MARKERS if marker in text)

    if strong and (bottoms_out_external or text_hits):
        return 1.0
    if strong or bottoms_out_external:
        return 0.8
    if text_hits >= 2:
        return 0.6
    if text_hits == 1:
        return 0.3
    return 0.0


def extract_signals(
    task_logs: str,
    dag_source: str,
    dag_id: str = "",
    task_id: str = "",
    history_merge_rate: Optional[float] = None,
    retrieval_hits: Optional[int] = None,
) -> Dict[str, Optional[float]]:
    """Computes every signal for one run.

    Optional signals (history_merge_rate, retrieval_support) come back as
    None when there's nothing to base them on, and the scorer drops them
    rather than guessing 0.5.
    """
    parsed = parse_log(task_logs)

    signals: Dict[str, Optional[float]] = {
        "stack_trace_present": signal_stack_trace_present(parsed),
        "line_number_matches_source": signal_line_number_matches_source(parsed, dag_source),
        "known_fix_pattern_match": signal_known_fix_pattern_match(parsed),
        "log_completeness": signal_log_completeness(parsed, task_logs, dag_id, task_id),
        "external_dependency_detected": signal_external_dependency_detected(parsed, task_logs),
        "history_merge_rate": (
            float(history_merge_rate) if history_merge_rate is not None else None
        ),
        "retrieval_support": (
            min(1.0, retrieval_hits / 5.0) if retrieval_hits is not None else None
        ),
    }
    return signals


# --------------------------------------------------------------------------
# Scoring
# --------------------------------------------------------------------------

def score_signals(signals: Dict[str, Optional[float]], weights_cfg: dict) -> dict:
    """Weighted-mean over available positive signals, minus penalties.

    Returns {"score", "tier", "used_signals", "contributions", "threshold"}.
    """
    cfg = normalize_weights_config(weights_cfg)
    signal_weights = cfg["signal_weights"]
    penalty_weights = cfg["penalty_weights"]

    used, contributions = {}, {}
    denom = 0.0
    numer = 0.0
    for name, weight in signal_weights.items():
        value = signals.get(name)
        if value is None:
            continue
        value = max(0.0, min(1.0, float(value)))
        used[name] = value
        numer += weight * value
        denom += weight
        contributions[name] = round(weight * value, 4)

    base = (numer / denom) if denom > 0 else 0.0

    penalty_total = 0.0
    for name, weight in penalty_weights.items():
        value = signals.get(name)
        if value is None:
            continue
        value = max(0.0, min(1.0, float(value)))
        used[name] = value
        penalty_total += weight * value
        contributions[name] = round(weight * value, 4)

    score = max(0.0, min(1.0, base + penalty_total))

    tiers = cfg["tier_thresholds"]
    if score >= tiers["high"]:
        tier = "high"
    elif score >= tiers["medium"]:
        tier = "medium"
    else:
        tier = "low"

    return {
        "score": round(score, 4),
        "tier": tier,
        "threshold": cfg["confidence_threshold"],
        "meets_threshold": score >= cfg["confidence_threshold"],
        "used_signals": used,
        "contributions": contributions,
        "base_before_penalty": round(base, 4),
    }


def score_run(task_logs: str, dag_source: str, weights_cfg: dict,
              dag_id: str = "", task_id: str = "",
              history_merge_rate: Optional[float] = None,
              retrieval_hits: Optional[int] = None) -> dict:
    """Convenience wrapper: extract signals then score them."""
    signals = extract_signals(
        task_logs=task_logs, dag_source=dag_source, dag_id=dag_id, task_id=task_id,
        history_merge_rate=history_merge_rate, retrieval_hits=retrieval_hits,
    )
    result = score_signals(signals, weights_cfg)
    result["signals"] = signals
    return result
