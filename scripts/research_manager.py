#!/usr/bin/env python3
"""Durable, bounded bookkeeping for long-running scientific research loops.

The command keeps compact decision state under ``.research/`` while leaving raw
logs, checkpoints, metrics, and other evidence at their original paths.  It is
deliberately deterministic: it records and reduces experiment state but never
forms hypotheses, interprets scientific results, or chooses the next experiment.

Typical flow::

    research-manager init --objective "Improve validation reward"
    research-manager plan experiment.json
    research-manager validate exp-001 --check "unit tests passed" --evidence reports/tests.txt
    research-manager record-launch exp-001 --job-id 64001 --log logs/exp-001.log
    research-manager sync exp-001
    research-manager record-summary exp-001 summary.json
    research-manager compare exp-001 --json
    research-manager decide exp-001 --decision refine --rationale "..." --next "..."
    research-manager handoff --json
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import math
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Callable, Iterator, Sequence


SCHEMA_VERSION = 1
MAX_JSON_BYTES = 1_000_000
MAX_HANDOFF_BYTES = 32_768
MAX_STATUS_BYTES = 32_768
MAX_COMPARE_BYTES = 65_536
MAX_HEALTH_BYTES = 65_536
DEFAULT_HEALTH_TAIL_BYTES = 4_194_304
MAX_HEALTH_TAIL_BYTES = 16_777_216
MAX_HEALTH_SCAN_LOGS = 128
MAX_HEALTH_OUTPUT_LOGS = 32
MAX_TEXT = 8_000
MAX_PATH_TEXT = 2_048
MAX_LIST_ITEMS = 128
MAX_ACTIVE_IN_HANDOFF = 10
MAX_COMPARE_RUNS = 20
MAX_RECENT_RUNS = 32
MAX_LEDGER_TAIL_BYTES = 262_144
MAX_DIAGNOSTICS = 100
MAX_SYNC_RUNS = 128

TRAINING_SIGNAL_PATTERNS = (
    (
        "nonfinite",
        "critical",
        re.compile(
            r"(?:\b(?:loss|grad(?:ient)?(?:[_ /-]?norm)?|reward|metric|perplexity)\b[^\n]{0,120}"
            r"\b(?:nan|inf(?:inity)?)\b|\b(?:nan|inf(?:inity)?)\b[^\n]{0,120}"
            r"\b(?:loss|grad(?:ient)?(?:[_ /-]?norm)?|reward|metric|perplexity)\b|\bnon[- ]finite\b)",
            re.IGNORECASE,
        ),
    ),
    (
        "numeric_overflow",
        "warning",
        re.compile(r"\b(?:found[_ ]inf|overflow detected|skipp(?:ed|ing) optimizer step)\b", re.IGNORECASE),
    ),
    (
        "out_of_memory",
        "critical",
        re.compile(r"\b(?:CUDA out of memory|OutOfMemoryError|CUDNN_STATUS_ALLOC_FAILED)\b", re.IGNORECASE),
    ),
    (
        "distributed",
        "critical",
        re.compile(
            r"\b(?:NCCL|GLOO|ProcessGroupNCCL|torch\.distributed|rendezvous|collective)\b"
            r"[^\n]{0,160}\b(?:error|failed|timeout|timed out|watchdog|abort|hang)\b|"
            r"\b(?:connection reset by peer|broken pipe)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "data_pipeline",
        "critical",
        re.compile(
            r"\bDataLoader worker\b[^\n]{0,120}\b(?:exited|killed|failed)\b|"
            r"\b(?:EOFError|too many open files)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "data_quality",
        "warning",
        re.compile(r"\b(?:corrupt(?:ed)? sample|invalid sample|skipping sample|empty batch)\b", re.IGNORECASE),
    ),
    (
        "checkpoint_or_storage",
        "critical",
        re.compile(
            r"\b(?:No space left on device|Disk quota exceeded)\b|"
            r"\b(?:checkpoint|state[_ ]?dict)\b[^\n]{0,160}"
            r"\b(?:corrupt|failed|failure|error|unexpected EOF|cannot|can't)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "explicit_instability",
        "critical",
        re.compile(
            r"\b(?:diverg(?:e|ed|ence|ing)|explod(?:e|ed|ing))\w*\b[^\n]{0,120}"
            r"\b(?:loss|grad(?:ient)?)\b|\b(?:loss|grad(?:ient)?)\b[^\n]{0,120}"
            r"\b(?:diverg(?:e|ed|ence|ing)|explod(?:e|ed|ing))\w*\b",
            re.IGNORECASE,
        ),
    ),
    (
        "exception",
        "critical",
        re.compile(
            r"Traceback \(most recent call last\)|\b(?:AssertionError|RuntimeError|Segmentation fault|SIGSEGV)\b|"
            r"slurmstepd:\s+error|(?:^|\s)Killed(?:\s|$)",
            re.IGNORECASE,
        ),
    ),
)

STEP_PATTERN = re.compile(r"\b(?:global_step|training_step|step)\s*[:=]\s*([0-9]+)\b", re.IGNORECASE)
LOSS_PATTERN = re.compile(
    r"\b(?:train(?:ing)?[_ /-]?)?loss\b\s*[:=]\s*"
    r"(nan|[-+]?inf(?:inity)?|[-+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:e[-+]?[0-9]+)?)",
    re.IGNORECASE,
)
GRAD_NORM_PATTERN = re.compile(
    r"\b(?:grad(?:ient)?[_ /-]?norm)\b\s*[:=]\s*"
    r"(nan|[-+]?inf(?:inity)?|[-+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:e[-+]?[0-9]+)?)",
    re.IGNORECASE,
)
THROUGHPUT_PATTERN = re.compile(
    r"\b(?:throughput|samples(?:/|_per_)s(?:ec)?|tokens(?:/|_per_)s(?:ec)?|steps(?:/|_per_)s(?:ec)?|it/s)\b"
    r"\s*[:=]?\s*([-+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:e[-+]?[0-9]+)?)",
    re.IGNORECASE,
)

RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
JOB_ID_RE = re.compile(r"^[0-9]+(?:_[0-9]+)?$")

RUN_STATES = {
    "PLANNED",
    "VALIDATED",
    "QUEUED",
    "RUNNING",
    "REDUCING",
    "READY",
    "FAILED",
    "CANCELLED",
    "DECIDED",
}
TERMINAL_RUN_STATES = {"READY", "FAILED", "CANCELLED", "DECIDED"}
ALLOWED_TRANSITIONS = {
    "PLANNED": {"VALIDATED", "FAILED", "CANCELLED"},
    "VALIDATED": {"QUEUED", "RUNNING", "FAILED", "CANCELLED"},
    "QUEUED": {"RUNNING", "REDUCING", "FAILED", "CANCELLED"},
    "RUNNING": {"REDUCING", "FAILED", "CANCELLED"},
    "REDUCING": {"READY", "FAILED", "CANCELLED"},
    "READY": {"DECIDED"},
    "FAILED": {"DECIDED"},
    "CANCELLED": {"DECIDED"},
    "DECIDED": set(),
}

DECISIONS = {"keep", "revert", "refine", "confirm", "redirect", "stop"}
SUMMARY_STATES = {"complete", "failed", "anomaly"}

SLURM_FAILURE_STATES = {
    "BOOT_FAIL",
    "CANCELLED",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "REVOKED",
    "SPECIAL_EXIT",
    "TIMEOUT",
}
SLURM_RUNNING_STATES = {"RUNNING", "COMPLETING", "STAGE_OUT"}
SLURM_QUEUED_STATES = {
    "PENDING",
    "CONFIGURING",
    "RESIZING",
    "REQUEUED",
    "REQUEUE_FED",
    "REQUEUE_HOLD",
    "SUSPENDED",
}


class ResearchManagerError(RuntimeError):
    """A concise, user-facing validation or workflow error."""


Runner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def compact_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def discover_root(start: Path) -> Path:
    current = start.expanduser().resolve()
    if current.is_file():
        current = current.parent
    for candidate in (current, *current.parents):
        if (candidate / ".git").exists():
            return candidate
    return current


def select_root(explicit: Path | None, cwd: Path) -> Path:
    if explicit is None:
        return discover_root(cwd)
    root = explicit.expanduser().resolve()
    if root.exists() and not root.is_dir():
        raise ResearchManagerError(f"project root is not a directory: {root}")
    return root


def research_dir(root: Path) -> Path:
    return root / ".research"


def state_path(root: Path) -> Path:
    return research_dir(root) / "state.json"


def run_dir(root: Path, run_id: str) -> Path:
    return research_dir(root) / "runs" / validate_run_id(run_id)


def health_path(root: Path, run_id: str) -> Path:
    return research_dir(root) / "health" / f"{validate_run_id(run_id)}.json"


def validate_run_id(run_id: str) -> str:
    if run_id in {".", ".."} or not RUN_ID_RE.fullmatch(run_id):
        raise ResearchManagerError(
            f"invalid run id {run_id!r}; use 1-64 letters, numbers, '.', '_', or '-'"
        )
    return run_id


def validate_job_id(job_id: str) -> str:
    if not JOB_ID_RE.fullmatch(job_id):
        raise ResearchManagerError(f"invalid Slurm job id: {job_id!r}")
    return job_id


def require_text(value: Any, field: str, *, maximum: int = MAX_TEXT) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ResearchManagerError(f"{field} must be a non-empty string")
    if len(value) > maximum:
        raise ResearchManagerError(f"{field} exceeds {maximum} characters")
    return value.strip()


def optional_text(value: Any, field: str, *, maximum: int = MAX_TEXT) -> str:
    if value in (None, ""):
        return ""
    return require_text(value, field, maximum=maximum)


def require_nonempty(value: Any, field: str) -> Any:
    if value is None or value == "" or value == [] or value == {}:
        raise ResearchManagerError(f"{field} must not be empty")
    return value


def validate_text_list(
    value: Any,
    field: str,
    *,
    required: bool = False,
    maximum: int = MAX_PATH_TEXT,
) -> list[str]:
    if not isinstance(value, list):
        raise ResearchManagerError(f"{field} must be a list")
    if required and not value:
        raise ResearchManagerError(f"{field} must contain at least one item")
    if len(value) > MAX_LIST_ITEMS:
        raise ResearchManagerError(f"{field} contains more than {MAX_LIST_ITEMS} items")
    return [require_text(item, f"{field}[{index}]", maximum=maximum) for index, item in enumerate(value)]


def validate_finite_numbers(value: Any, field: str) -> None:
    if isinstance(value, bool):
        return
    if isinstance(value, (int, float)):
        if not math.isfinite(float(value)):
            raise ResearchManagerError(f"{field} contains a non-finite number")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise ResearchManagerError(f"{field} contains an invalid key")
            validate_finite_numbers(item, f"{field}.{key}")
        return
    if isinstance(value, list):
        if len(value) > MAX_LIST_ITEMS:
            raise ResearchManagerError(f"{field} contains more than {MAX_LIST_ITEMS} items")
        for index, item in enumerate(value):
            validate_finite_numbers(item, f"{field}[{index}]")
        return
    if isinstance(value, str):
        if len(value) > MAX_TEXT:
            raise ResearchManagerError(f"{field} contains an oversized string")
        return
    if value is None:
        return
    raise ResearchManagerError(f"{field} contains unsupported data of type {type(value).__name__}")


def read_json(path: Path, *, label: str = "JSON") -> dict[str, Any]:
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise ResearchManagerError(f"cannot read {label} file {path}: {exc}") from exc
    if size > MAX_JSON_BYTES:
        raise ResearchManagerError(
            f"{label} file {path} is {size} bytes; keep structured inputs under {MAX_JSON_BYTES} bytes "
            "and reference larger evidence by path"
        )
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ResearchManagerError(f"cannot parse {label} file {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ResearchManagerError(f"{label} file {path} must contain one JSON object")
    return value


def atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(compact_json(value) + "\n", encoding="utf-8")
        os.replace(temporary, path)
    except OSError as exc:
        with contextlib.suppress(OSError):
            temporary.unlink()
        raise ResearchManagerError(f"cannot write {path}: {exc}") from exc


def append_jsonl(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = compact_json(value) + "\n"
    if len(line.encode("utf-8")) > MAX_JSON_BYTES:
        raise ResearchManagerError(f"event for {path.name} is too large")
    try:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as exc:
        raise ResearchManagerError(f"cannot append to {path}: {exc}") from exc


@contextlib.contextmanager
def locked(root: Path) -> Iterator[None]:
    directory = research_dir(root)
    directory.mkdir(parents=True, exist_ok=True)
    lock_path = directory / ".lock"
    try:
        with lock_path.open("a+") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            yield
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except OSError as exc:
        raise ResearchManagerError(f"cannot lock research state at {lock_path}: {exc}") from exc


def initial_state(objective: str) -> dict[str, Any]:
    now = utc_now()
    return {
        "schema": SCHEMA_VERSION,
        "objective": require_text(objective, "objective"),
        "strategy": "",
        "current_hypothesis": "",
        "status": "initialized",
        "next_decision": "",
        "active_run_ids": [],
        "recent_run_ids": [],
        "created_at": now,
        "updated_at": now,
    }


def validate_state(value: dict[str, Any]) -> dict[str, Any]:
    if value.get("schema") != SCHEMA_VERSION:
        raise ResearchManagerError("unsupported or missing research state schema")
    require_text(value.get("objective"), "state.objective")
    for field in ("strategy", "current_hypothesis", "status", "next_decision"):
        optional_text(value.get(field, ""), f"state.{field}")
    active = value.get("active_run_ids")
    recent = value.get("recent_run_ids")
    if not isinstance(active, list) or not isinstance(recent, list):
        raise ResearchManagerError("state run ID fields must be lists")
    for run_id in [*active, *recent]:
        validate_run_id(run_id)
    return value


def load_state(root: Path) -> dict[str, Any]:
    path = state_path(root)
    if not path.exists():
        raise ResearchManagerError(
            f"research loop is not initialized at {root}; run 'research-manager init --objective ...'"
        )
    return validate_state(read_json(path, label="state"))


def save_state(root: Path, state: dict[str, Any]) -> None:
    state["updated_at"] = utc_now()
    validate_state(state)
    atomic_write_json(state_path(root), state)


def load_record(root: Path, run_id: str) -> dict[str, Any]:
    path = run_dir(root, run_id) / "record.json"
    if not path.exists():
        raise ResearchManagerError(f"unknown research run: {run_id}")
    record = read_json(path, label="run record")
    if record.get("run_id") != run_id or record.get("status") not in RUN_STATES:
        raise ResearchManagerError(f"invalid run record for {run_id}")
    return record


def save_record(root: Path, record: dict[str, Any]) -> None:
    run_id = validate_run_id(str(record.get("run_id", "")))
    if record.get("status") not in RUN_STATES:
        raise ResearchManagerError(f"invalid status for {run_id}: {record.get('status')!r}")
    record["updated_at"] = utc_now()
    atomic_write_json(run_dir(root, run_id) / "record.json", record)


def touch_run_in_state(state: dict[str, Any], run_id: str, status: str) -> None:
    active = [item for item in state["active_run_ids"] if item != run_id]
    if status not in TERMINAL_RUN_STATES:
        active.append(run_id)
    state["active_run_ids"] = active
    recent = [item for item in state["recent_run_ids"] if item != run_id]
    recent.insert(0, run_id)
    state["recent_run_ids"] = recent[:MAX_RECENT_RUNS]


def event(root: Path, run_id: str, event_name: str, **details: Any) -> None:
    payload: dict[str, Any] = {
        "schema": SCHEMA_VERSION,
        "at": utc_now(),
        "run_id": run_id,
        "event": event_name,
    }
    if details:
        payload["details"] = details
    append_jsonl(research_dir(root) / "runs.jsonl", payload)


def transition(
    root: Path,
    state: dict[str, Any],
    record: dict[str, Any],
    target: str,
    *,
    reason: str = "",
    event_name: str = "transition",
    event_details: dict[str, Any] | None = None,
) -> None:
    target = target.upper()
    current = record["status"]
    if target == current:
        return
    if target not in RUN_STATES:
        raise ResearchManagerError(f"unknown run state: {target}")
    if target not in ALLOWED_TRANSITIONS[current]:
        raise ResearchManagerError(f"illegal run transition: {current} -> {target}")
    record["status"] = target
    if reason:
        record["status_reason"] = require_text(reason, "transition reason")
    save_record(root, record)
    touch_run_in_state(state, record["run_id"], target)
    details = {"from": current, "to": target}
    if reason:
        details["reason"] = reason
    if event_details:
        details.update(event_details)
    event(root, record["run_id"], event_name, **details)


def validate_primary_metric(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ResearchManagerError("spec.primary_metric must be an object")
    name = require_text(value.get("name"), "spec.primary_metric.name", maximum=256)
    direction = require_text(value.get("direction"), "spec.primary_metric.direction", maximum=16).lower()
    if direction not in {"maximize", "minimize"}:
        raise ResearchManagerError("spec.primary_metric.direction must be 'maximize' or 'minimize'")
    validated = dict(value)
    validated["name"] = name
    validated["direction"] = direction
    validate_finite_numbers(validated, "spec.primary_metric")
    return validated


def validate_spec(value: dict[str, Any]) -> dict[str, Any]:
    required = {
        "run_id",
        "hypothesis",
        "change",
        "baseline",
        "treatment",
        "primary_metric",
        "success_criteria",
        "failure_criteria",
        "evaluation",
    }
    missing = sorted(field for field in required if field not in value)
    if missing:
        raise ResearchManagerError(f"spec is missing required fields: {', '.join(missing)}")
    validated = dict(value)
    validated["schema"] = SCHEMA_VERSION
    validated["run_id"] = validate_run_id(str(value["run_id"]))
    validated["hypothesis"] = require_text(value["hypothesis"], "spec.hypothesis")
    validated["change"] = require_text(value["change"], "spec.change")
    for field in ("baseline", "treatment", "success_criteria", "failure_criteria", "evaluation"):
        require_nonempty(value[field], f"spec.{field}")
        validate_finite_numbers(value[field], f"spec.{field}")
    validated["primary_metric"] = validate_primary_metric(value["primary_metric"])
    if "evidence_paths" in value:
        validated["evidence_paths"] = validate_text_list(value["evidence_paths"], "spec.evidence_paths")
    for field in ("resources", "budget", "secondary_metrics", "notes", "tags"):
        if field in value:
            validate_finite_numbers(value[field], f"spec.{field}")
    if len(compact_json(validated).encode("utf-8")) > MAX_JSON_BYTES:
        raise ResearchManagerError("validated spec is too large; reference bulky material by path")
    return validated


def validate_metric_map(value: Any, field: str, *, required: bool) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ResearchManagerError(f"{field} must be an object")
    if required and not value:
        raise ResearchManagerError(f"{field} must not be empty")
    for name, metric in value.items():
        require_text(name, f"{field} key", maximum=256)
        if isinstance(metric, bool) or not isinstance(metric, (int, float, dict)):
            raise ResearchManagerError(f"{field}.{name} must be numeric or an object containing a numeric value")
        if isinstance(metric, dict):
            if "value" not in metric or isinstance(metric["value"], bool) or not isinstance(
                metric["value"], (int, float)
            ):
                raise ResearchManagerError(f"{field}.{name}.value must be numeric")
        validate_finite_numbers(metric, f"{field}.{name}")
    return value


def validate_summary(value: dict[str, Any]) -> dict[str, Any]:
    required = {
        "status",
        "primary_metrics",
        "baseline_delta",
        "sample_counts",
        "runtime",
        "failure_modes",
        "anomalies",
        "evidence_paths",
        "needs_judgment",
    }
    missing = sorted(field for field in required if field not in value)
    if missing:
        raise ResearchManagerError(f"summary is missing required fields: {', '.join(missing)}")
    validated = dict(value)
    validated["schema"] = SCHEMA_VERSION
    status = require_text(value["status"], "summary.status", maximum=32).lower()
    if status not in SUMMARY_STATES:
        raise ResearchManagerError(f"summary.status must be one of: {', '.join(sorted(SUMMARY_STATES))}")
    validated["status"] = status
    validated["primary_metrics"] = validate_metric_map(
        value["primary_metrics"], "summary.primary_metrics", required=status == "complete"
    )
    validated["baseline_delta"] = validate_metric_map(
        value["baseline_delta"], "summary.baseline_delta", required=False
    )
    if not isinstance(value["sample_counts"], dict) or (status == "complete" and not value["sample_counts"]):
        raise ResearchManagerError("summary.sample_counts must be a non-empty object for completed runs")
    if not isinstance(value["runtime"], dict) or not value["runtime"]:
        raise ResearchManagerError("summary.runtime must be a non-empty object")
    validate_finite_numbers(value["sample_counts"], "summary.sample_counts")
    validate_finite_numbers(value["runtime"], "summary.runtime")
    validated["failure_modes"] = validate_text_list(value["failure_modes"], "summary.failure_modes")
    validated["anomalies"] = validate_text_list(value["anomalies"], "summary.anomalies")
    validated["evidence_paths"] = validate_text_list(
        value["evidence_paths"], "summary.evidence_paths", required=True
    )
    if not isinstance(value["needs_judgment"], bool):
        raise ResearchManagerError("summary.needs_judgment must be true or false")
    if (status == "anomaly" or validated["anomalies"]) and not value["needs_judgment"]:
        raise ResearchManagerError("anomalous summaries must set needs_judgment to true")
    if "notes" in value:
        validated["notes"] = optional_text(value["notes"], "summary.notes")
    if "secondary_metrics" in value:
        validated["secondary_metrics"] = validate_metric_map(
            value["secondary_metrics"], "summary.secondary_metrics", required=False
        )
    if "health_checks" in value:
        if not isinstance(value["health_checks"], dict) or not value["health_checks"]:
            raise ResearchManagerError("summary.health_checks must be a non-empty object when provided")
        validate_finite_numbers(value["health_checks"], "summary.health_checks")
        validated["health_checks"] = value["health_checks"]
    if len(compact_json(validated).encode("utf-8")) > MAX_JSON_BYTES:
        raise ResearchManagerError("validated summary is too large; keep raw evidence in referenced files")
    return validated


def init_research(root: Path, objective: str | None) -> dict[str, Any]:
    with locked(root):
        path = state_path(root)
        if path.exists():
            state = load_state(root)
            if objective and require_text(objective, "objective") != state["objective"]:
                raise ResearchManagerError(
                    "research loop already exists with a different objective; use 'checkpoint --objective ...' explicitly"
                )
            return {"created": False, "root": str(root), "state": state}
        if not objective:
            raise ResearchManagerError("--objective is required when initializing a new research loop")
        state = initial_state(objective)
        (research_dir(root) / "runs").mkdir(parents=True, exist_ok=True)
        save_state(root, state)
        append_jsonl(
            research_dir(root) / "runs.jsonl",
            {"schema": SCHEMA_VERSION, "at": utc_now(), "event": "research_initialized"},
        )
        return {"created": True, "root": str(root), "state": state}


def plan_run(root: Path, spec_path: Path) -> dict[str, Any]:
    spec = validate_spec(read_json(spec_path, label="experiment spec"))
    run_id = spec["run_id"]
    with locked(root):
        state = load_state(root)
        directory = run_dir(root, run_id)
        if (directory / "record.json").exists():
            raise ResearchManagerError(f"research run already exists: {run_id}")
        now = utc_now()
        record = {
            "schema": SCHEMA_VERSION,
            "run_id": run_id,
            "status": "PLANNED",
            "created_at": now,
            "updated_at": now,
            "job_ids": [],
            "log_paths": [],
            "artifact_paths": [],
        }
        atomic_write_json(directory / "spec.json", spec)
        save_record(root, record)
        touch_run_in_state(state, run_id, "PLANNED")
        state["current_hypothesis"] = spec["hypothesis"]
        state["status"] = f"planned {run_id}"
        state["next_decision"] = f"validate {run_id} before launch"
        save_state(root, state)
        event(root, run_id, "planned")
        return compact_run(root, record, include_summary=False)


def validate_run(root: Path, run_id: str, checks: Sequence[str], evidence_paths: Sequence[str]) -> dict[str, Any]:
    run_id = validate_run_id(run_id)
    if not checks:
        raise ResearchManagerError("at least one --check is required; record the tests or smoke checks actually run")
    validated_checks = [require_text(item, "validation check", maximum=1_000) for item in checks]
    validated_evidence = [require_text(item, "validation evidence", maximum=MAX_PATH_TEXT) for item in evidence_paths]
    with locked(root):
        state = load_state(root)
        record = load_record(root, run_id)
        validate_spec(read_json(run_dir(root, run_id) / "spec.json", label="stored experiment spec"))
        if record["status"] != "PLANNED":
            raise ResearchManagerError(f"run {run_id} must be PLANNED before validation")
        record["validation"] = {
            "checks": validated_checks,
            "evidence_paths": validated_evidence,
            "validated_at": utc_now(),
        }
        transition(
            root,
            state,
            record,
            "VALIDATED",
            event_name="validated",
            event_details={"checks": validated_checks, "evidence_paths": validated_evidence},
        )
        state["status"] = f"validated {run_id}"
        state["next_decision"] = f"launch {run_id} or revise its spec"
        save_state(root, state)
        return compact_run(root, record, include_summary=False)


def record_launch(
    root: Path,
    run_id: str,
    job_ids: Sequence[str],
    log_paths: Sequence[str],
    artifact_paths: Sequence[str],
) -> dict[str, Any]:
    run_id = validate_run_id(run_id)
    validated_jobs = list(dict.fromkeys(validate_job_id(item) for item in job_ids))
    if not validated_jobs:
        raise ResearchManagerError("at least one --job-id is required")
    logs = list(dict.fromkeys(require_text(item, "log path", maximum=MAX_PATH_TEXT) for item in log_paths))
    artifacts = list(
        dict.fromkeys(require_text(item, "artifact path", maximum=MAX_PATH_TEXT) for item in artifact_paths)
    )
    with locked(root):
        state = load_state(root)
        record = load_record(root, run_id)
        if record["status"] != "VALIDATED":
            raise ResearchManagerError(f"run {run_id} must be VALIDATED before recording a launch")
        record["job_ids"] = validated_jobs
        record["log_paths"] = logs
        record["artifact_paths"] = artifacts
        record["launched_at"] = utc_now()
        transition(
            root,
            state,
            record,
            "QUEUED",
            event_name="launch_recorded",
            event_details={"job_ids": validated_jobs, "log_paths": logs, "artifact_paths": artifacts},
        )
        state["status"] = f"queued {run_id}"
        state["next_decision"] = f"wait for a state change in {run_id}; do not poll unchanged state"
        save_state(root, state)
        return compact_run(root, record, include_summary=False)


def set_run_state(root: Path, run_id: str, target: str, reason: str) -> dict[str, Any]:
    run_id = validate_run_id(run_id)
    target = target.upper()
    with locked(root):
        state = load_state(root)
        record = load_record(root, run_id)
        transition(root, state, record, target, reason=reason)
        state["status"] = f"{run_id} {target.lower()}"
        save_state(root, state)
        return compact_run(root, record, include_summary=True)


def default_runner(args: Sequence[str]) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(list(args), check=False, capture_output=True, text=True)
    except FileNotFoundError as exc:
        raise ResearchManagerError(f"required command is unavailable: {args[0]}") from exc


def scheduler_target(states: Sequence[str], *, has_summary: bool) -> str | None:
    normalized = [state.strip().upper().split(maxsplit=1)[0].rstrip("+") for state in states]
    if any(state in SLURM_FAILURE_STATES for state in normalized):
        return "FAILED"
    known = [state for state in normalized if state and state != "UNKNOWN"]
    if not known:
        return None
    if all(state == "COMPLETED" for state in known) and len(known) == len(normalized):
        return "READY" if has_summary else "REDUCING"
    if any(state in SLURM_RUNNING_STATES for state in known):
        return "RUNNING"
    if any(state in SLURM_QUEUED_STATES for state in known):
        return "QUEUED"
    return None


def sync_runs(
    root: Path,
    run_ids: Sequence[str],
    *,
    cluster_manager: str,
    runner: Runner = default_runner,
) -> dict[str, Any]:
    with locked(root):
        state = load_state(root)
        selected = [validate_run_id(item) for item in run_ids] if run_ids else list(state["active_run_ids"])
        if len(selected) > MAX_SYNC_RUNS:
            raise ResearchManagerError(
                f"sync accepts at most {MAX_SYNC_RUNS} runs at once; select a smaller active batch"
            )
        records = [load_record(root, run_id) for run_id in selected]
        jobs = list(dict.fromkeys(job_id for record in records for job_id in record.get("job_ids", [])))
        if not jobs:
            raise ResearchManagerError("selected runs have no recorded Slurm job IDs")
        command = [cluster_manager, "status", *jobs, "--no-state", "--json"]
        result = runner(command)
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "cluster-manager failed").strip().splitlines()
            raise ResearchManagerError(detail[-1][:500] if detail else "cluster-manager failed")
        try:
            report = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise ResearchManagerError(f"cluster-manager returned invalid JSON: {exc}") from exc
        jobs_by_id = {str(item.get("job_id")): item for item in report.get("jobs", [])}
        updates: list[dict[str, Any]] = []
        for record in records:
            run_id = record["run_id"]
            if record["status"] in TERMINAL_RUN_STATES:
                updates.append({"run_id": run_id, "status": record["status"], "changed": False})
                continue
            job_states = {
                job_id: str(jobs_by_id.get(job_id, {}).get("state", "UNKNOWN"))
                for job_id in record.get("job_ids", [])
            }
            target = scheduler_target(
                list(job_states.values()),
                has_summary=(run_dir(root, run_id) / "summary.json").exists(),
            )
            changed = False
            if target and target != record["status"]:
                if target in ALLOWED_TRANSITIONS[record["status"]]:
                    transition(
                        root,
                        state,
                        record,
                        target,
                        reason="scheduler state",
                        event_name="scheduler_sync",
                        event_details={"job_states": job_states},
                    )
                    changed = True
                elif target == "FAILED" and "FAILED" in ALLOWED_TRANSITIONS[record["status"]]:
                    transition(
                        root,
                        state,
                        record,
                        "FAILED",
                        reason="scheduler failure",
                        event_name="scheduler_sync",
                        event_details={"job_states": job_states},
                    )
                    changed = True
            updates.append(
                {"run_id": run_id, "status": record["status"], "changed": changed, "job_states": job_states}
            )
        state["status"] = "scheduler state synchronized"
        save_state(root, state)
        return {
            "schema": SCHEMA_VERSION,
            "checked_at": report.get("checked_at", utc_now()),
            "updates": updates,
            "anomaly": bool(report.get("anomaly")),
            "warnings": report.get("warnings", []),
        }


def record_summary(root: Path, run_id: str, summary_path: Path) -> dict[str, Any]:
    run_id = validate_run_id(run_id)
    summary = validate_summary(read_json(summary_path, label="experiment summary"))
    if health_path(root, run_id).exists():
        health = read_json(health_path(root, run_id), label="stored training-health report")
        if health.get("needs_judgment") and not summary["needs_judgment"]:
            raise ResearchManagerError(
                f"run {run_id} has unresolved training-health signals; summary.needs_judgment must be true"
            )
    with locked(root):
        state = load_state(root)
        record = load_record(root, run_id)
        if record["status"] not in {"VALIDATED", "QUEUED", "RUNNING", "REDUCING"}:
            raise ResearchManagerError(
                f"run {run_id} cannot accept a summary while {record['status']}; expected an executed run"
            )
        atomic_write_json(run_dir(root, run_id) / "summary.json", summary)
        record["summary_status"] = summary["status"]
        record["needs_judgment"] = summary["needs_judgment"]
        target = "FAILED" if summary["status"] == "failed" else "READY"
        current = record["status"]
        if target not in ALLOWED_TRANSITIONS[current]:
            if target == "READY" and current in {"VALIDATED", "QUEUED", "RUNNING"}:
                # A reducer may finish before the scheduler is synchronized. Record
                # the missing REDUCING phase explicitly instead of bypassing it.
                if "REDUCING" in ALLOWED_TRANSITIONS[current]:
                    transition(root, state, record, "REDUCING", reason="summary supplied")
                else:
                    if current == "VALIDATED":
                        transition(root, state, record, "RUNNING", reason="external execution recorded")
                    transition(root, state, record, "REDUCING", reason="summary supplied")
            elif target == "FAILED" and target not in ALLOWED_TRANSITIONS[record["status"]]:
                raise ResearchManagerError(f"cannot mark {run_id} failed from {record['status']}")
        transition(
            root,
            state,
            record,
            target,
            event_name="summary_recorded",
            event_details={
                "summary_status": summary["status"],
                "needs_judgment": summary["needs_judgment"],
                "evidence_paths": summary["evidence_paths"],
            },
        )
        state["status"] = f"{run_id} ready for scientific judgment" if target == "READY" else f"{run_id} failed"
        state["next_decision"] = f"inspect evidence and decide {run_id}"
        save_state(root, state)
        return compact_run(root, record, include_summary=True)


def decide_run(
    root: Path,
    run_id: str,
    decision: str,
    rationale: str,
    next_step: str,
) -> dict[str, Any]:
    run_id = validate_run_id(run_id)
    decision = decision.lower()
    if decision not in DECISIONS:
        raise ResearchManagerError(f"decision must be one of: {', '.join(sorted(DECISIONS))}")
    rationale = require_text(rationale, "decision rationale")
    next_step = optional_text(next_step, "decision next step")
    with locked(root):
        state = load_state(root)
        record = load_record(root, run_id)
        if record["status"] not in {"READY", "FAILED", "CANCELLED"}:
            raise ResearchManagerError(f"run {run_id} is not ready for a decision (status={record['status']})")
        if record["status"] == "READY" and not (run_dir(root, run_id) / "summary.json").exists():
            raise ResearchManagerError(f"run {run_id} has no structured summary")
        decision_record = {
            "schema": SCHEMA_VERSION,
            "at": utc_now(),
            "run_id": run_id,
            "decision": decision,
            "rationale": rationale,
            "next_step": next_step,
        }
        append_jsonl(research_dir(root) / "decisions.jsonl", decision_record)
        record["decision"] = decision
        record["decision_at"] = decision_record["at"]
        transition(
            root,
            state,
            record,
            "DECIDED",
            event_name="decision_recorded",
            event_details={"decision": decision},
        )
        state["status"] = f"decided {run_id}: {decision}"
        state["next_decision"] = next_step
        save_state(root, state)
        return decision_record


def checkpoint_state(root: Path, updates: dict[str, str]) -> dict[str, Any]:
    allowed = {"objective", "strategy", "current_hypothesis", "status", "next_decision"}
    supplied = {key: value for key, value in updates.items() if value is not None}
    if not supplied:
        raise ResearchManagerError("checkpoint requires at least one state field")
    unexpected = sorted(set(supplied) - allowed)
    if unexpected:
        raise ResearchManagerError(f"unsupported checkpoint fields: {', '.join(unexpected)}")
    with locked(root):
        state = load_state(root)
        for field, value in supplied.items():
            state[field] = require_text(value, f"checkpoint.{field}")
        save_state(root, state)
        append_jsonl(
            research_dir(root) / "runs.jsonl",
            {"schema": SCHEMA_VERSION, "at": utc_now(), "event": "checkpoint", "fields": sorted(supplied)},
        )
        return state


def load_spec(root: Path, run_id: str) -> dict[str, Any]:
    return validate_spec(read_json(run_dir(root, run_id) / "spec.json", label="stored experiment spec"))


def load_summary(root: Path, run_id: str) -> dict[str, Any] | None:
    path = run_dir(root, run_id) / "summary.json"
    return validate_summary(read_json(path, label="stored experiment summary")) if path.exists() else None


def clean_health_line(line: str) -> str:
    cleaned = line.strip()
    if len(cleaned) > 400:
        return f"{cleaned[:280]}…{cleaned[-119:]}"
    return cleaned


def add_health_signal(
    signals: dict[str, dict[str, Any]],
    category: str,
    severity: str,
    sample: str,
    *,
    count: int = 1,
) -> None:
    item = signals.setdefault(category, {"category": category, "severity": severity, "count": 0, "samples": []})
    if severity == "critical":
        item["severity"] = "critical"
    item["count"] += count
    cleaned = clean_health_line(sample)
    if cleaned and cleaned not in item["samples"]:
        item["samples"] = [*item["samples"], cleaned][-3:]


def numeric_value(raw: str) -> float:
    lowered = raw.strip().lower()
    if lowered == "nan":
        return math.nan
    if lowered in {"inf", "+inf", "infinity", "+infinity"}:
        return math.inf
    if lowered in {"-inf", "-infinity"}:
        return -math.inf
    return float(raw)


def series_stats(values: Sequence[float]) -> dict[str, Any]:
    finite = [value for value in values if math.isfinite(value)]
    result: dict[str, Any] = {"observations": len(values), "finite": len(finite)}
    nonfinite = len(values) - len(finite)
    if nonfinite:
        result["nonfinite"] = nonfinite
    if finite:
        result.update(
            {
                "first": finite[0],
                "last": finite[-1],
                "min": min(finite),
                "max": max(finite),
                "median": median(finite),
            }
        )
    return result


def relative_spike(values: Sequence[float], *, factor: float, absolute_floor: float = 0.0) -> bool:
    finite = [abs(value) for value in values if math.isfinite(value)]
    if len(finite) < 5:
        return False
    baseline = median(finite[:-1])
    last = finite[-1]
    return last > absolute_floor and last > factor * max(baseline, 1e-12)


def relative_collapse(values: Sequence[float], *, factor: float) -> bool:
    finite = [value for value in values if math.isfinite(value) and value >= 0]
    if len(finite) < 5:
        return False
    baseline = median(finite[:-1])
    return baseline > 0 and finite[-1] < factor * baseline


def display_evidence_path(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path)


def scan_training_log(
    path: Path,
    *,
    root: Path,
    run_status: str,
    tail_bytes: int,
    stale_seconds: float,
    previous: dict[str, Any],
    now_epoch: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    shown_path = display_evidence_path(path, root)
    signals: dict[str, dict[str, Any]] = {}
    result: dict[str, Any] = {"path": shown_path}
    if not path.exists():
        add_health_signal(signals, "missing_log", "warning", f"recorded log is missing: {shown_path}")
        result.update({"status": "warning", "signals": list(signals.values()), "missing": True})
        snapshot = {
            "path": shown_path,
            "size": 0,
            "mtime_epoch": 0,
            "last_step": None,
            "unchanged_since_epoch": previous.get("unchanged_since_epoch", now_epoch),
        }
        return result, snapshot
    if not path.is_file():
        add_health_signal(signals, "invalid_log", "critical", f"recorded log is not a file: {shown_path}")
        result.update({"status": "critical", "signals": list(signals.values()), "not_file": True})
        snapshot = {
            "path": shown_path,
            "size": 0,
            "mtime_epoch": 0,
            "last_step": None,
            "unchanged_since_epoch": previous.get("unchanged_since_epoch", now_epoch),
        }
        return result, snapshot

    try:
        stat = path.stat()
        start = max(0, stat.st_size - tail_bytes)
        with path.open("rb") as handle:
            handle.seek(start)
            raw = handle.read(tail_bytes)
    except OSError as exc:
        add_health_signal(signals, "log_read", "critical", f"cannot read {shown_path}: {exc}")
        result.update({"status": "critical", "signals": list(signals.values())})
        snapshot = {
            "path": shown_path,
            "size": 0,
            "mtime_epoch": 0,
            "last_step": None,
            "unchanged_since_epoch": previous.get("unchanged_since_epoch", now_epoch),
        }
        return result, snapshot

    text = raw.decode("utf-8", errors="replace")
    lines = text.splitlines()
    for line in lines:
        for category, severity, pattern in TRAINING_SIGNAL_PATTERNS:
            if pattern.search(line):
                add_health_signal(signals, category, severity, line)

    steps = [int(match.group(1)) for line in lines for match in STEP_PATTERN.finditer(line)]
    losses = [numeric_value(match.group(1)) for line in lines for match in LOSS_PATTERN.finditer(line)]
    grad_norms = [numeric_value(match.group(1)) for line in lines for match in GRAD_NORM_PATTERN.finditer(line)]
    throughputs = [numeric_value(match.group(1)) for line in lines for match in THROUGHPUT_PATTERN.finditer(line)]

    if any(not math.isfinite(value) for value in losses):
        add_health_signal(signals, "nonfinite", "critical", "parsed a non-finite training loss")
    if any(not math.isfinite(value) for value in grad_norms):
        add_health_signal(signals, "nonfinite", "critical", "parsed a non-finite gradient norm")
    if relative_spike(losses, factor=10.0):
        add_health_signal(signals, "loss_spike", "warning", "latest parsed loss is >10x the prior median")
    if relative_spike(grad_norms, factor=100.0, absolute_floor=100.0):
        add_health_signal(signals, "gradient_spike", "warning", "latest gradient norm is >100x the prior median")
    if relative_collapse(throughputs, factor=0.1):
        add_health_signal(signals, "throughput_collapse", "warning", "latest throughput is <10% of the prior median")
    if len(steps) >= 2 and steps[-1] < max(steps[:-1]):
        add_health_signal(signals, "step_regression", "warning", "parsed training step moved backwards")

    last_step = steps[-1] if steps else None
    same_progress = previous.get("size") == stat.st_size and previous.get("last_step") == last_step
    unchanged_since = float(previous.get("unchanged_since_epoch", now_epoch)) if same_progress else now_epoch
    log_age = max(0.0, now_epoch - stat.st_mtime)
    unchanged_seconds = max(0.0, now_epoch - unchanged_since)
    if run_status == "RUNNING" and stale_seconds > 0:
        if log_age >= stale_seconds:
            add_health_signal(
                signals,
                "stalled_progress",
                "warning",
                f"running log has not changed for {int(log_age)} seconds",
            )
        elif same_progress and unchanged_seconds >= stale_seconds:
            add_health_signal(
                signals,
                "stalled_progress",
                "warning",
                f"training step and log size are unchanged for {int(unchanged_seconds)} seconds",
            )

    ordered_signals = sorted(
        signals.values(),
        key=lambda item: (0 if item["severity"] == "critical" else 1, item["category"]),
    )
    critical = any(item["severity"] == "critical" for item in ordered_signals)
    result.update(
        {
            "status": "critical" if critical else "warning" if ordered_signals else "ok",
            "size": stat.st_size,
            "mtime": datetime.fromtimestamp(stat.st_mtime, timezone.utc).replace(microsecond=0).isoformat(),
            "age_seconds": int(log_age),
            "bytes_scanned": len(raw),
            "tail_truncated": start > 0,
            "last_step": last_step,
            "step_observations": len(steps),
            "loss": series_stats(losses),
            "gradient_norm": series_stats(grad_norms),
            "throughput": series_stats(throughputs),
            "signals": ordered_signals,
        }
    )
    snapshot = {
        "path": shown_path,
        "size": stat.st_size,
        "mtime_epoch": stat.st_mtime,
        "last_step": last_step,
        "unchanged_since_epoch": unchanged_since,
    }
    return result, snapshot


def bound_health_report(report: dict[str, Any]) -> dict[str, Any]:
    bounded = dict(report)
    if len(compact_json(bounded).encode("utf-8")) <= MAX_HEALTH_BYTES:
        return bounded
    logs = []
    for item in bounded.get("logs", []):
        reduced = dict(item)
        reduced["signals"] = [
            {
                **{key: signal[key] for key in ("category", "severity", "count")},
                "samples": signal.get("samples", [])[-1:],
            }
            for signal in item.get("signals", [])
        ]
        logs.append(reduced)
    bounded["logs"] = logs
    while len(compact_json(bounded).encode("utf-8")) > MAX_HEALTH_BYTES and len(bounded["logs"]) > 1:
        bounded["logs"].pop()
        bounded["omitted_log_details"] = bounded.get("omitted_log_details", 0) + 1
    if len(compact_json(bounded).encode("utf-8")) > MAX_HEALTH_BYTES:
        bounded["logs"] = [
            {
                key: item[key]
                for key in ("path", "status", "size", "mtime", "last_step", "signals")
                if key in item
            }
            for item in bounded.get("logs", [])
        ]
    if len(compact_json(bounded).encode("utf-8")) > MAX_HEALTH_BYTES:
        raise ResearchManagerError("bounded training-health report unexpectedly exceeds its output budget")
    return bounded


def training_health(
    root: Path,
    run_id: str,
    *,
    tail_bytes: int = DEFAULT_HEALTH_TAIL_BYTES,
    stale_seconds: float = 1_800.0,
    now_epoch: float | None = None,
) -> dict[str, Any]:
    run_id = validate_run_id(run_id)
    if tail_bytes <= 0 or tail_bytes > MAX_HEALTH_TAIL_BYTES:
        raise ResearchManagerError(f"tail bytes must be between 1 and {MAX_HEALTH_TAIL_BYTES}")
    if stale_seconds < 0:
        raise ResearchManagerError("stale seconds must be non-negative")
    now_epoch = time.time() if now_epoch is None else now_epoch
    record = load_record(root, run_id)
    summary = load_summary(root, run_id)
    paths = list(record.get("log_paths", []))
    if summary:
        paths.extend(
            item
            for item in summary.get("evidence_paths", [])
            if Path(item).suffix.lower() in {".log", ".out", ".err"}
        )
    paths = list(dict.fromkeys(paths))
    selected_paths = paths[:MAX_HEALTH_SCAN_LOGS]

    previous_report: dict[str, Any] = {}
    if health_path(root, run_id).exists():
        previous_report = read_json(health_path(root, run_id), label="stored training-health report")
    previous_by_path = {
        str(item.get("path")): item
        for item in previous_report.get("log_snapshots", [])
        if isinstance(item, dict) and item.get("path")
    }

    scanned: list[dict[str, Any]] = []
    snapshots: list[dict[str, Any]] = []
    for raw_path in selected_paths:
        path = Path(raw_path).expanduser()
        path = path if path.is_absolute() else root / path
        shown_path = display_evidence_path(path, root)
        result, snapshot = scan_training_log(
            path,
            root=root,
            run_status=record["status"],
            tail_bytes=tail_bytes,
            stale_seconds=stale_seconds,
            previous=previous_by_path.get(shown_path, {}),
            now_epoch=now_epoch,
        )
        scanned.append(result)
        snapshots.append(snapshot)

    aggregate: dict[str, dict[str, Any]] = {}
    for log in scanned:
        for signal in log.get("signals", []):
            item = aggregate.setdefault(
                signal["category"],
                {"category": signal["category"], "severity": signal["severity"], "count": 0, "logs": []},
            )
            if signal["severity"] == "critical":
                item["severity"] = "critical"
            item["count"] += int(signal.get("count", 1))
            if log["path"] not in item["logs"]:
                item["logs"].append(log["path"])

    if record["status"] == "FAILED":
        aggregate["run_failed"] = {
            "category": "run_failed",
            "severity": "critical",
            "count": 1,
            "logs": [],
        }
    if not paths and record["status"] in {"RUNNING", "REDUCING", "READY", "FAILED"}:
        aggregate["missing_log_registration"] = {
            "category": "missing_log_registration",
            "severity": "warning",
            "count": 1,
            "logs": [],
        }
    omitted_scan = max(0, len(paths) - len(selected_paths))
    if omitted_scan:
        aggregate["unscanned_logs"] = {
            "category": "unscanned_logs",
            "severity": "warning",
            "count": omitted_scan,
            "logs": [],
        }

    aggregate_signals = sorted(
        aggregate.values(),
        key=lambda item: (0 if item["severity"] == "critical" else 1, item["category"]),
    )
    critical_count = sum(item["count"] for item in aggregate_signals if item["severity"] == "critical")
    warning_count = sum(item["count"] for item in aggregate_signals if item["severity"] == "warning")
    health_status = "critical" if critical_count else "warning" if warning_count else "ok"
    detail_order = sorted(scanned, key=lambda item: ({"critical": 0, "warning": 1, "ok": 2}[item["status"]], item["path"]))
    shown_logs = detail_order[:MAX_HEALTH_OUTPUT_LOGS]
    report: dict[str, Any] = {
        "schema": SCHEMA_VERSION,
        "checked_at": datetime.fromtimestamp(now_epoch, timezone.utc).replace(microsecond=0).isoformat(),
        "run_id": run_id,
        "run_status": record["status"],
        "status": health_status,
        "healthy": critical_count == 0,
        "needs_judgment": bool(critical_count or warning_count),
        "critical_signal_count": critical_count,
        "warning_signal_count": warning_count,
        "signals": aggregate_signals,
        "registered_log_count": len(paths),
        "scanned_log_count": len(selected_paths),
        "logs": shown_logs,
        "raw_evidence_retained": True,
        "note": (
            "This is a heuristic index. Inspect the referenced raw logs and metrics before diagnosing or "
            "making a scientific decision."
        ),
    }
    omitted_details = len(detail_order) - len(shown_logs)
    if omitted_details:
        report["omitted_log_details"] = omitted_details
    if omitted_scan:
        report["omitted_unscanned_logs"] = omitted_scan

    stored_report = dict(report)
    stored_report["log_snapshots"] = snapshots
    atomic_write_json(health_path(root, run_id), stored_report)

    with locked(root):
        current_record = load_record(root, run_id)
        previous_status = current_record.get("health_status")
        current_record["health_status"] = health_status
        current_record["health_checked_at"] = report["checked_at"]
        current_record["health_report_path"] = display_evidence_path(health_path(root, run_id), root)
        if report["needs_judgment"]:
            current_record["needs_judgment"] = True
        save_record(root, current_record)
        state = load_state(root)
        touch_run_in_state(state, run_id, current_record["status"])
        if report["needs_judgment"] and current_record["status"] not in TERMINAL_RUN_STATES:
            state["status"] = f"training health {health_status} for {run_id}"
            state["next_decision"] = f"inspect training-health evidence for {run_id}"
        save_state(root, state)
        if previous_status != health_status:
            event(
                root,
                run_id,
                "training_health_changed",
                previous_status=previous_status or "unknown",
                status=health_status,
                critical_signal_count=critical_count,
                warning_signal_count=warning_count,
            )
    return bound_health_report(report)


def metric_number(value: Any) -> float | int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, dict) and isinstance(value.get("value"), (int, float)) and not isinstance(
        value.get("value"), bool
    ):
        return value["value"]
    return None


def compact_run(root: Path, record: dict[str, Any], *, include_summary: bool) -> dict[str, Any]:
    spec = load_spec(root, record["run_id"])
    primary_metric = {
        key: spec["primary_metric"][key]
        for key in ("name", "direction", "unit", "aggregation")
        if key in spec["primary_metric"]
    }
    result: dict[str, Any] = {
        "run_id": record["run_id"],
        "status": record["status"],
        "hypothesis": spec["hypothesis"],
        "change": spec["change"],
        "primary_metric": primary_metric,
        "job_ids": record.get("job_ids", []),
        "log_paths": record.get("log_paths", []),
        "artifact_paths": record.get("artifact_paths", []),
        "updated_at": record.get("updated_at", ""),
    }
    for field in (
        "needs_judgment",
        "decision",
        "status_reason",
        "health_status",
        "health_checked_at",
        "health_report_path",
    ):
        if field in record:
            result[field] = record[field]
    if include_summary:
        summary = load_summary(root, record["run_id"])
        if summary:
            result["summary"] = {
                "status": summary["status"],
                "primary_metrics": summary["primary_metrics"],
                "baseline_delta": summary["baseline_delta"],
                "sample_counts": summary["sample_counts"],
                "runtime": summary["runtime"],
                "failure_modes": summary["failure_modes"],
                "anomalies": summary["anomalies"],
                "evidence_paths": summary["evidence_paths"],
                "needs_judgment": summary["needs_judgment"],
            }
            if "health_checks" in summary:
                result["summary"]["health_checks"] = summary["health_checks"]
    return result


def tail_jsonl(path: Path, limit: int) -> list[dict[str, Any]]:
    if limit <= 0 or not path.exists():
        return []
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - MAX_LEDGER_TAIL_BYTES))
            raw = handle.read(MAX_LEDGER_TAIL_BYTES)
    except OSError as exc:
        raise ResearchManagerError(f"cannot read {path}: {exc}") from exc
    lines = raw.decode("utf-8", errors="replace").splitlines()
    if size > MAX_LEDGER_TAIL_BYTES and lines:
        lines = lines[1:]
    values: list[dict[str, Any]] = []
    for line in reversed(lines):
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            values.append(value)
        if len(values) >= limit:
            break
    return list(reversed(values))


def clip_text(value: Any, maximum: int) -> Any:
    if isinstance(value, str):
        return value if len(value) <= maximum else value[: maximum - 1] + "…"
    if isinstance(value, list):
        return [clip_text(item, maximum) for item in value]
    if isinstance(value, dict):
        return {key: clip_text(item, maximum) for key, item in value.items()}
    return value


def limited_mapping(
    value: dict[str, Any],
    limit: int,
    *,
    preferred: Sequence[str] = (),
) -> tuple[dict[str, Any], int]:
    ordered = [key for key in preferred if key in value]
    ordered.extend(key for key in sorted(value) if key not in ordered)
    selected = ordered[:limit]
    return {key: value[key] for key in selected}, max(0, len(ordered) - len(selected))


def limited_list(value: Sequence[Any], limit: int) -> tuple[list[Any], int]:
    return list(value[:limit]), max(0, len(value) - limit)


def handoff_run(value: dict[str, Any]) -> dict[str, Any]:
    result = {
        key: value[key]
        for key in (
            "run_id",
            "status",
            "hypothesis",
            "change",
            "primary_metric",
            "updated_at",
            "needs_judgment",
            "decision",
            "status_reason",
            "health_status",
            "health_checked_at",
            "health_report_path",
        )
        if key in value
    }
    for field in ("job_ids", "log_paths", "artifact_paths"):
        items, omitted = limited_list(value.get(field, []), 16)
        if items:
            result[field] = items
        if omitted:
            result[f"omitted_{field}"] = omitted
    summary = value.get("summary")
    if isinstance(summary, dict):
        preferred_metric = str(value.get("primary_metric", {}).get("name", ""))
        preferred = [preferred_metric] if preferred_metric else []
        reduced_summary = {
            key: summary[key]
            for key in ("status", "needs_judgment")
            if key in summary
        }
        for field, limit in (
            ("primary_metrics", 16),
            ("baseline_delta", 16),
            ("sample_counts", 16),
            ("runtime", 16),
            ("health_checks", 16),
        ):
            source = summary.get(field, {})
            if isinstance(source, dict):
                selected, omitted = limited_mapping(source, limit, preferred=preferred)
                reduced_summary[field] = selected
                if omitted:
                    reduced_summary[f"omitted_{field}"] = omitted
        for field, limit in (("failure_modes", 8), ("anomalies", 8), ("evidence_paths", 16)):
            selected, omitted = limited_list(summary.get(field, []), limit)
            reduced_summary[field] = selected
            if omitted:
                reduced_summary[f"omitted_{field}"] = omitted
        result["summary"] = reduced_summary
    return clip_text(result, 1_200)


def bound_handoff(payload: dict[str, Any]) -> dict[str, Any]:
    bounded = dict(payload)
    bounded["active_runs"] = [handoff_run(item) for item in payload.get("active_runs", [])]
    bounded["recent_runs"] = [handoff_run(item) for item in payload.get("recent_runs", [])]
    bounded = clip_text(bounded, 1_200)
    while len(compact_json(bounded).encode("utf-8")) > MAX_HANDOFF_BYTES:
        if bounded.get("recent_decisions"):
            bounded["recent_decisions"].pop(0)
            bounded["omitted_recent_decisions"] = bounded.get("omitted_recent_decisions", 0) + 1
            continue
        if len(bounded.get("recent_runs", [])) > 1:
            bounded["recent_runs"].pop()
            bounded["omitted_recent_runs"] = bounded.get("omitted_recent_runs", 0) + 1
            continue
        if len(bounded.get("active_runs", [])) > 1:
            bounded["active_runs"].pop()
            bounded["omitted_active_runs"] = bounded.get("omitted_active_runs", 0) + 1
            continue
        bounded["active_runs"] = [
            {
                key: item[key]
                for key in ("run_id", "status", "primary_metric", "job_ids", "needs_judgment")
                if key in item
            }
            for item in bounded.get("active_runs", [])
        ]
        bounded["recent_runs"] = [
            {
                key: item[key]
                for key in ("run_id", "status", "primary_metric", "decision", "health_status")
                if key in item
            }
            for item in bounded.get("recent_runs", [])
        ]
        bounded = clip_text(bounded, 300)
        break
    if len(compact_json(bounded).encode("utf-8")) > MAX_HANDOFF_BYTES:
        raise ResearchManagerError("bounded handoff unexpectedly exceeds its output budget")
    return bounded


def build_handoff(root: Path, limit: int) -> dict[str, Any]:
    state = load_state(root)
    limit = max(1, min(limit, 20))
    active_ids = state["active_run_ids"][-MAX_ACTIVE_IN_HANDOFF:]
    recent_ids = [run_id for run_id in state["recent_run_ids"] if run_id not in active_ids][:limit]
    active_runs = [compact_run(root, load_record(root, run_id), include_summary=True) for run_id in active_ids]
    recent_runs = [compact_run(root, load_record(root, run_id), include_summary=True) for run_id in recent_ids]
    decisions = tail_jsonl(research_dir(root) / "decisions.jsonl", limit)
    payload = {
        "schema": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "root": str(root),
        "research": {
            field: state[field]
            for field in ("objective", "strategy", "current_hypothesis", "status", "next_decision")
        },
        "active_run_count": len(state["active_run_ids"]),
        "active_runs": active_runs,
        "recent_runs": recent_runs,
        "recent_decisions": decisions,
        "evidence_policy": (
            "This is a bounded index, not the evidence itself. Inspect referenced evidence for anomalies, "
            "borderline results, and scientific decisions."
        ),
    }
    omitted_active = len(state["active_run_ids"]) - len(active_ids)
    if omitted_active:
        payload["omitted_active_runs"] = omitted_active
    return bound_handoff(payload)


def status_report(root: Path) -> dict[str, Any]:
    state = load_state(root)
    selected_ids = state["active_run_ids"][-MAX_ACTIVE_IN_HANDOFF:]
    active = [handoff_run(compact_run(root, load_record(root, run_id), include_summary=False)) for run_id in selected_ids]
    report = {
        "schema": SCHEMA_VERSION,
        "root": str(root),
        "objective": clip_text(state["objective"], 2_000),
        "status": clip_text(state["status"], 1_000),
        "next_decision": clip_text(state["next_decision"], 2_000),
        "active_run_count": len(state["active_run_ids"]),
        "active_runs": active,
        "updated_at": state["updated_at"],
    }
    omitted = len(state["active_run_ids"]) - len(selected_ids)
    if omitted:
        report["omitted_active_runs"] = omitted
    if len(compact_json(report).encode("utf-8")) > MAX_STATUS_BYTES:
        report["active_runs"] = [
            {
                key: item[key]
                for key in ("run_id", "status", "job_ids", "needs_judgment", "health_status")
                if key in item
            }
            for item in active
        ]
    if len(compact_json(report).encode("utf-8")) > MAX_STATUS_BYTES:
        raise ResearchManagerError("bounded status unexpectedly exceeds its output budget")
    return report


def compare_runs(root: Path, run_ids: Sequence[str]) -> dict[str, Any]:
    if not run_ids:
        raise ResearchManagerError("compare requires at least one run ID")
    if len(run_ids) > MAX_COMPARE_RUNS:
        raise ResearchManagerError(f"compare accepts at most {MAX_COMPARE_RUNS} runs")
    rows: list[dict[str, Any]] = []
    metric_names: set[str] = set()
    for raw_run_id in run_ids:
        run_id = validate_run_id(raw_run_id)
        record = load_record(root, run_id)
        spec = load_spec(root, run_id)
        summary = load_summary(root, run_id)
        row: dict[str, Any] = {
            "run_id": run_id,
            "status": record["status"],
            "hypothesis": spec["hypothesis"],
            "primary_metric": {
                key: spec["primary_metric"][key]
                for key in ("name", "direction", "unit", "aggregation")
                if key in spec["primary_metric"]
            },
        }
        if summary:
            primary_name = spec["primary_metric"]["name"]
            selected_metrics, omitted_metrics = limited_mapping(
                summary["primary_metrics"], 32, preferred=[primary_name]
            )
            selected_deltas, omitted_deltas = limited_mapping(
                summary["baseline_delta"], 32, preferred=[primary_name]
            )
            metrics = {name: metric_number(value) for name, value in selected_metrics.items()}
            deltas = {name: metric_number(value) for name, value in selected_deltas.items()}
            metric_names.update(metrics)
            sample_counts, omitted_counts = limited_mapping(summary["sample_counts"], 16)
            runtime, omitted_runtime = limited_mapping(summary["runtime"], 16)
            failure_modes, omitted_failures = limited_list(summary["failure_modes"], 8)
            anomalies, omitted_anomalies = limited_list(summary["anomalies"], 8)
            evidence_paths, omitted_evidence = limited_list(summary["evidence_paths"], 16)
            row.update(
                {
                    "metrics": metrics,
                    "baseline_delta": deltas,
                    "sample_counts": sample_counts,
                    "runtime": runtime,
                    "failure_modes": failure_modes,
                    "anomalies": anomalies,
                    "needs_judgment": summary["needs_judgment"],
                    "evidence_paths": evidence_paths,
                }
            )
            for field, omitted in (
                ("metrics", omitted_metrics),
                ("baseline_delta", omitted_deltas),
                ("sample_counts", omitted_counts),
                ("runtime", omitted_runtime),
                ("failure_modes", omitted_failures),
                ("anomalies", omitted_anomalies),
                ("evidence_paths", omitted_evidence),
            ):
                if omitted:
                    row[f"omitted_{field}"] = omitted
        else:
            row["summary_missing"] = True
        rows.append(row)
    selected_metric_names, omitted_metric_names = limited_list(sorted(metric_names), 32)
    report = {
        "schema": SCHEMA_VERSION,
        "metric_names": selected_metric_names,
        "runs": rows,
        "interpretation_required": True,
        "note": "This command aligns recorded measurements; it does not choose a scientific winner.",
    }
    if omitted_metric_names:
        report["omitted_metric_names"] = omitted_metric_names
    report = clip_text(report, 1_000)
    if len(compact_json(report).encode("utf-8")) > MAX_COMPARE_BYTES:
        minimal_rows: list[dict[str, Any]] = []
        for row in report["runs"]:
            primary_name = str(row.get("primary_metric", {}).get("name", ""))
            minimal = {
                key: row[key]
                for key in ("run_id", "status", "primary_metric", "needs_judgment", "summary_missing")
                if key in row
            }
            if primary_name:
                minimal["metrics"] = {primary_name: row.get("metrics", {}).get(primary_name)}
                minimal["baseline_delta"] = {
                    primary_name: row.get("baseline_delta", {}).get(primary_name)
                }
            minimal["anomalies"] = row.get("anomalies", [])[:3]
            minimal["evidence_paths"] = row.get("evidence_paths", [])[:3]
            minimal["detail_omitted_for_budget"] = True
            minimal_rows.append(minimal)
        report["runs"] = minimal_rows
        report["detail_omitted_for_budget"] = True
    if len(compact_json(report).encode("utf-8")) > MAX_COMPARE_BYTES:
        raise ResearchManagerError("bounded comparison unexpectedly exceeds its output budget")
    return report


def path_is_local_reference(value: str) -> bool:
    return "://" not in value and not value.startswith(("s3:", "gs:"))


def resolve_evidence(root: Path, value: str) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else root / path


def build_evidence_manifest(
    root: Path,
    run_id: str,
    spec: dict[str, Any],
    record: dict[str, Any],
    summary: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    sources: list[tuple[str, str]] = []
    sources.extend(("spec", item) for item in spec.get("evidence_paths", []))
    sources.extend(("validation", item) for item in record.get("validation", {}).get("evidence_paths", []))
    sources.extend(("log", item) for item in record.get("log_paths", []))
    sources.extend(("artifact", item) for item in record.get("artifact_paths", []))
    if summary:
        sources.extend(("summary", item) for item in summary.get("evidence_paths", []))
    if health_path(root, run_id).exists():
        sources.append(("training_health", display_evidence_path(health_path(root, run_id), root)))

    grouped: dict[str, set[str]] = {}
    for source, value in sources:
        grouped.setdefault(value, set()).add(source)
    manifest: list[dict[str, Any]] = []
    for value, source_names in grouped.items():
        item: dict[str, Any] = {"path": value, "sources": sorted(source_names)}
        if path_is_local_reference(value):
            path = resolve_evidence(root, value)
            try:
                stat = path.stat()
            except FileNotFoundError:
                item["exists"] = False
            except OSError as exc:
                item.update({"exists": None, "stat_error": str(exc)[:500]})
            else:
                item.update(
                    {
                        "exists": True,
                        "kind": "file" if path.is_file() else "directory" if path.is_dir() else "other",
                        "size": stat.st_size,
                        "mtime": datetime.fromtimestamp(stat.st_mtime, timezone.utc)
                        .replace(microsecond=0)
                        .isoformat(),
                    }
                )
        else:
            item.update({"exists": None, "remote": True})
        manifest.append(item)
    return manifest


def inspect_run(root: Path, run_id: str, section: str = "all") -> dict[str, Any]:
    run_id = validate_run_id(run_id)
    if section not in {"all", "spec", "record", "summary", "health", "evidence"}:
        raise ResearchManagerError("inspect section must be all, spec, record, summary, health, or evidence")
    record = load_record(root, run_id)
    spec = load_spec(root, run_id)
    summary = load_summary(root, run_id)
    health = (
        read_json(health_path(root, run_id), label="stored training-health report")
        if health_path(root, run_id).exists()
        else None
    )
    evidence = build_evidence_manifest(root, run_id, spec, record, summary)
    sections = {
        "spec": spec,
        "record": record,
        "summary": summary,
        "health": health,
        "evidence": evidence,
    }
    report: dict[str, Any] = {
        "schema": SCHEMA_VERSION,
        "run_id": run_id,
        "section": section,
        "lossless_structured_retrieval": True,
        "raw_evidence_included": False,
        "note": "Structured records are complete; raw evidence remains at every path in the evidence manifest.",
    }
    if section == "all":
        report.update(sections)
    else:
        report[section] = sections[section]
    return report


def doctor(root: Path) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    try:
        state = load_state(root)
    except ResearchManagerError as exc:
        return {"schema": SCHEMA_VERSION, "healthy": False, "errors": [str(exc)], "warnings": []}
    all_ids = list(dict.fromkeys([*state["active_run_ids"], *state["recent_run_ids"]]))
    runs_root = research_dir(root) / "runs"
    if runs_root.exists():
        for path in runs_root.iterdir():
            if path.is_dir() and RUN_ID_RE.fullmatch(path.name) and path.name not in all_ids:
                all_ids.append(path.name)
    for run_id in all_ids:
        try:
            record = load_record(root, run_id)
            load_spec(root, run_id)
            summary = load_summary(root, run_id)
            if run_id in state["active_run_ids"] and record["status"] in TERMINAL_RUN_STATES:
                errors.append(f"{run_id}: terminal run is listed as active")
            if run_id not in state["active_run_ids"] and record["status"] not in TERMINAL_RUN_STATES:
                errors.append(f"{run_id}: non-terminal run is missing from active_run_ids")
            evidence: list[str] = []
            evidence.extend(record.get("log_paths", []))
            evidence.extend(record.get("artifact_paths", []))
            evidence.extend(record.get("validation", {}).get("evidence_paths", []))
            if record.get("health_report_path"):
                evidence.append(record["health_report_path"])
            if summary:
                evidence.extend(summary["evidence_paths"])
            if health_path(root, run_id).exists():
                health = read_json(health_path(root, run_id), label="stored training-health report")
                if health.get("run_id") != run_id:
                    errors.append(f"{run_id}: training-health report has the wrong run_id")
            for item in dict.fromkeys(evidence):
                if path_is_local_reference(item) and not resolve_evidence(root, item).exists():
                    warnings.append(f"{run_id}: referenced evidence is currently missing: {item}")
        except ResearchManagerError as exc:
            errors.append(f"{run_id}: {exc}")
    for ledger_name in ("runs.jsonl", "decisions.jsonl"):
        ledger = research_dir(root) / ledger_name
        if not ledger.exists():
            continue
        line_number = 0
        try:
            with ledger.open(encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, start=1):
                    if not line.strip():
                        continue
                    value = json.loads(line)
                    if not isinstance(value, dict):
                        raise ValueError("entry is not an object")
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
            errors.append(f"{ledger_name}: invalid entry near line {line_number}: {exc}")
    error_count = len(errors)
    warning_count = len(warnings)
    report = {
        "schema": SCHEMA_VERSION,
        "healthy": not error_count,
        "run_count": len(all_ids),
        "active_run_count": len(state["active_run_ids"]),
        "errors": errors[:MAX_DIAGNOSTICS],
        "warnings": warnings[:MAX_DIAGNOSTICS],
    }
    if error_count > MAX_DIAGNOSTICS:
        report["omitted_errors"] = error_count - MAX_DIAGNOSTICS
    if warning_count > MAX_DIAGNOSTICS:
        report["omitted_warnings"] = warning_count - MAX_DIAGNOSTICS
    return report


SPEC_TEMPLATE = {
    "run_id": "exp-001",
    "hypothesis": "A precise, falsifiable statement.",
    "change": "The smallest coherent code or configuration change.",
    "baseline": {"run_id": "baseline", "configuration": "control"},
    "treatment": {"configuration": "single changed factor"},
    "primary_metric": {"name": "validation_score", "direction": "maximize", "unit": "score"},
    "success_criteria": {"minimum_delta": 0.01, "confirmation_required": True},
    "failure_criteria": {"regression_below": -0.01, "runtime_multiplier_above": 1.5},
    "evaluation": {"benchmark": "name", "slice": "validation", "seeds": [1, 2, 3]},
    "resources": {"gpus": 1},
    "budget": {"max_wall_minutes": 60},
    "evidence_paths": ["path/to/pre-run-notes.md"],
}

SUMMARY_TEMPLATE = {
    "status": "complete",
    "primary_metrics": {"validation_score": {"value": 0.0, "std": 0.0}},
    "baseline_delta": {"validation_score": 0.0},
    "sample_counts": {"examples": 0, "seeds": 0},
    "runtime": {"wall_seconds": 0, "gpu_hours": 0.0, "peak_memory_gib": 0.0},
    "failure_modes": [],
    "anomalies": [],
    "health_checks": {
        "nonfinite": "pass",
        "step_progress": "pass",
        "throughput": "pass",
        "gradient_norm": "pass",
        "distributed": "pass",
        "data_pipeline": "pass",
        "checkpoint_and_storage": "pass",
    },
    "evidence_paths": ["path/to/full-metrics.json", "path/to/run.log"],
    "needs_judgment": True,
    "notes": "Bounded interpretation notes; keep detailed evidence in referenced files.",
}


def render(value: dict[str, Any], *, as_json: bool, kind: str = "generic") -> None:
    if as_json:
        print(compact_json(value))
        return
    if kind == "status":
        print(f"OBJECTIVE {value['objective']}")
        print(f"STATUS {value['status']}")
        if value.get("next_decision"):
            print(f"NEXT {value['next_decision']}")
        for run in value.get("active_runs", []):
            jobs = ",".join(run.get("job_ids", [])) or "-"
            print(f"RUN {run['run_id']} {run['status']} jobs={jobs}")
        return
    print(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Bounded, durable state for scientific research loops.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  %(prog)s init --objective "Improve validation score"
  %(prog)s template spec > experiment.json
  %(prog)s plan experiment.json
  %(prog)s validate exp-001 --check "tests passed" --evidence reports/tests.txt
  %(prog)s record-launch exp-001 --job-id 64001 --log logs/exp-001.log
  %(prog)s sync exp-001 --json
  %(prog)s health exp-001 --json
  %(prog)s record-summary exp-001 summary.json
  %(prog)s inspect exp-001 --section evidence --json
  %(prog)s handoff --json
""",
    )
    parser.add_argument("--root", type=Path, help="project root; defaults to the nearest Git root")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="initialize durable research state")
    init.add_argument("--objective", help="research objective; required on first use")
    init.add_argument("--json", action="store_true")

    template = subparsers.add_parser("template", help="emit a JSON input template")
    template.add_argument("kind", choices=("spec", "summary"))

    plan = subparsers.add_parser("plan", help="register a predeclared experiment spec")
    plan.add_argument("spec", type=Path)
    plan.add_argument("--json", action="store_true")

    validate = subparsers.add_parser("validate", help="record pre-launch validation evidence")
    validate.add_argument("run_id")
    validate.add_argument("--check", action="append", default=[], help="test or smoke check actually completed")
    validate.add_argument("--evidence", action="append", default=[], help="path to validation evidence")
    validate.add_argument("--json", action="store_true")

    launch = subparsers.add_parser("record-launch", help="record an already submitted Slurm launch")
    launch.add_argument("run_id")
    launch.add_argument("--job-id", action="append", default=[])
    launch.add_argument("--log", action="append", default=[])
    launch.add_argument("--artifact", action="append", default=[])
    launch.add_argument("--json", action="store_true")

    run_transition = subparsers.add_parser("transition", help="record a controlled run-state transition")
    run_transition.add_argument("run_id")
    run_transition.add_argument("state", choices=sorted(RUN_STATES))
    run_transition.add_argument("--reason", default="")
    run_transition.add_argument("--json", action="store_true")

    sync = subparsers.add_parser("sync", help="synchronize recorded runs from compact Slurm state")
    sync.add_argument("run_ids", nargs="*")
    sync.add_argument(
        "--cluster-manager",
        default=os.environ.get("CLUSTER_MANAGER", "cluster-manager"),
        help="cluster-manager executable",
    )
    sync.add_argument("--json", action="store_true")

    health = subparsers.add_parser("health", help="scan recorded training logs for bounded health signals")
    health.add_argument("run_id")
    health.add_argument("--tail-bytes", type=int, default=DEFAULT_HEALTH_TAIL_BYTES)
    health.add_argument(
        "--stale-seconds",
        type=float,
        default=1_800.0,
        help="warn when a RUNNING log has no progress for this many seconds; 0 disables",
    )
    health.add_argument("--json", action="store_true")

    summary = subparsers.add_parser("record-summary", help="record a bounded result summary with evidence paths")
    summary.add_argument("run_id")
    summary.add_argument("summary", type=Path)
    summary.add_argument("--json", action="store_true")

    decide = subparsers.add_parser("decide", help="record a human/high-reasoning scientific decision")
    decide.add_argument("run_id")
    decide.add_argument("--decision", required=True, choices=sorted(DECISIONS))
    decide.add_argument("--rationale", required=True)
    decide.add_argument("--next", default="", dest="next_step")
    decide.add_argument("--json", action="store_true")

    checkpoint = subparsers.add_parser("checkpoint", help="update compact cross-session research state")
    checkpoint.add_argument("--objective")
    checkpoint.add_argument("--strategy")
    checkpoint.add_argument("--hypothesis", dest="current_hypothesis")
    checkpoint.add_argument("--status")
    checkpoint.add_argument("--next-decision")
    checkpoint.add_argument("--json", action="store_true")

    status = subparsers.add_parser("status", help="show compact current state")
    status.add_argument("--json", action="store_true")

    handoff = subparsers.add_parser("handoff", help="emit bounded canonical context for a new conversation")
    handoff.add_argument("--limit", type=int, default=5)
    handoff.add_argument("--json", action="store_true")

    compare = subparsers.add_parser("compare", help="align recorded metrics without interpreting them")
    compare.add_argument("run_ids", nargs="+")
    compare.add_argument("--json", action="store_true")

    inspect = subparsers.add_parser("inspect", help="retrieve complete stored state or its evidence manifest")
    inspect.add_argument("run_id")
    inspect.add_argument(
        "--section",
        choices=("all", "spec", "record", "summary", "health", "evidence"),
        default="all",
    )
    inspect.add_argument("--json", action="store_true")

    diagnose = subparsers.add_parser("doctor", help="validate research state and evidence references")
    diagnose.add_argument("--json", action="store_true")
    return parser


def execute(args: argparse.Namespace, root: Path) -> tuple[dict[str, Any], str]:
    if args.command == "init":
        return init_research(root, args.objective), "generic"
    if args.command == "plan":
        return plan_run(root, args.spec), "generic"
    if args.command == "validate":
        return validate_run(root, args.run_id, args.check, args.evidence), "generic"
    if args.command == "record-launch":
        return record_launch(root, args.run_id, args.job_id, args.log, args.artifact), "generic"
    if args.command == "transition":
        return set_run_state(root, args.run_id, args.state, args.reason), "generic"
    if args.command == "sync":
        return sync_runs(root, args.run_ids, cluster_manager=args.cluster_manager), "generic"
    if args.command == "health":
        return training_health(
            root,
            args.run_id,
            tail_bytes=args.tail_bytes,
            stale_seconds=args.stale_seconds,
        ), "generic"
    if args.command == "record-summary":
        return record_summary(root, args.run_id, args.summary), "generic"
    if args.command == "decide":
        return decide_run(root, args.run_id, args.decision, args.rationale, args.next_step), "generic"
    if args.command == "checkpoint":
        updates = {
            "objective": args.objective,
            "strategy": args.strategy,
            "current_hypothesis": args.current_hypothesis,
            "status": args.status,
            "next_decision": args.next_decision,
        }
        return checkpoint_state(root, updates), "generic"
    if args.command == "status":
        return status_report(root), "status"
    if args.command == "handoff":
        return build_handoff(root, args.limit), "generic"
    if args.command == "compare":
        return compare_runs(root, args.run_ids), "generic"
    if args.command == "inspect":
        return inspect_run(root, args.run_id, args.section), "generic"
    if args.command == "doctor":
        return doctor(root), "generic"
    raise ResearchManagerError(f"unsupported command: {args.command}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "template":
        print(json.dumps(SPEC_TEMPLATE if args.kind == "spec" else SUMMARY_TEMPLATE, indent=2, sort_keys=True))
        return 0
    try:
        root = select_root(args.root, Path.cwd())
        value, kind = execute(args, root)
        render(value, as_json=bool(getattr(args, "json", False)), kind=kind)
        if args.command in {"doctor", "health"} and not value.get("healthy", False):
            return 1
        return 0
    except ResearchManagerError as exc:
        print(f"research-manager: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
