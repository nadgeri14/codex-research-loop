#!/usr/bin/env python3
"""Compact Slurm status and event-driven log monitoring.

The CLI replaces recurring hand-written combinations of ``squeue``, ``sacct``,
``scontrol``, ``tail``, and ``rg`` with stable commands:

    scripts/cluster_manager.py status 64001 64002 --logs --json
    scripts/cluster_manager.py watch 64001 --monitor-spec .research/monitors/RUN/spec.json \
        --state-file .research/monitors/RUN/state.json --until wake --timeout 0 --json
    scripts/cluster_manager.py gpus --available-only --json
    scripts/cluster_manager.py resources --json

State is kept outside /tmp so separate invocations can report only meaningful
changes.  The tool never submits, cancels, requeues, or edits a Slurm job.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import getpass
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Callable, Iterator, Sequence


SCHEMA_VERSION = 1
MONITOR_STATE_SCHEMA = 2
MONITOR_SPEC_SCHEMA = 1
MAX_MONITOR_SPEC_BYTES = 1_000_000
MAX_MONITOR_STATE_BYTES = 1_000_000
DEFAULT_LUNA_PACKET_BYTES = 16_384
MIN_LUNA_PACKET_BYTES = 4_096
MAX_LUNA_PACKET_BYTES = 65_536
MAX_EVENT_EVIDENCE_CHARS = 4_000
MAX_PARSED_EVENTS_PER_LOG = 128
MAX_DEDUPE_ENTRIES = 512
MAX_ONCE_ENTRIES = 4_096
JOB_ID_RE = re.compile(r"^[0-9]+(?:_[0-9]+)?$")
TERMINAL_STATES = {
    "BOOT_FAIL",
    "CANCELLED",
    "COMPLETED",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "REVOKED",
    "SPECIAL_EXIT",
    "TIMEOUT",
}
FAILURE_STATES = TERMINAL_STATES - {"COMPLETED"}

DEFAULT_ERROR_PATTERNS = (
    r"Traceback \(most recent call last\)",
    r"\b(?:FATAL|ABORT)\b",
    r"\b(?:CUDA out of memory|OutOfMemoryError|RayTaskError)\b",
    r"(?:\b(?:loss|grad(?:ient)?(?:[_ /-]?norm)?|reward|metric|perplexity)\b[^\n]{0,120}"
    r"\b(?:nan|inf(?:inity)?)\b|\b(?:nan|inf(?:inity)?)\b[^\n]{0,120}"
    r"\b(?:loss|grad(?:ient)?(?:[_ /-]?norm)?|reward|metric|perplexity)\b|\bnon[- ]finite\b)",
    r"\b(?:diverg(?:e|ed|ence|ing)|explod(?:e|ed|ing))\w*\b[^\n]{0,120}"
    r"\b(?:loss|grad(?:ient)?)\b",
    r"\b(?:NCCL|GLOO|ProcessGroupNCCL|torch\.distributed|rendezvous|collective)\b"
    r"[^\n]{0,160}\b(?:error|failed|timeout|timed out|watchdog|abort|hang)\b",
    r"\bDataLoader worker\b[^\n]{0,120}\b(?:exited|killed|failed)\b",
    r"\b(?:No space left on device|Disk quota exceeded|too many open files)\b",
    r"\b(?:checkpoint|state[_ ]?dict)\b[^\n]{0,160}"
    r"\b(?:corrupt|failed|failure|error|unexpected EOF|cannot|can't)\b",
    r"slurmstepd:\s+error",
    r"(?:^|\s)Killed(?:\s|$)",
)
DEFAULT_MILESTONE_PATTERNS = (
    r"\b(?:saving|saved)\b.*\b(?:model|checkpoint)\b",
    r"\baudit\b.*\b(?:pass(?:ed)?|complete(?:d)?)\b",
    r"\bcomplete(?:d)? successfully\b",
    r"\[(?:done|ok)\]",
)

# Ordinary training telemetry is deliberately separate from wake-worthy
# milestones.  The previous implementation treated every step/loss/gradient
# line as a milestone, so ``watch --until event`` returned control to an LLM on
# healthy progress.  These expressions update compact state but never wake an
# agent by themselves.
PROGRESS_STEP_PATTERN = re.compile(
    r"\b(?:global_step|training_step|step)\s*[:=]\s*([0-9]+)\b", re.IGNORECASE
)
PROGRESS_LOSS_PATTERN = re.compile(
    r"\b(?:train(?:ing)?[_ /-]?)?loss\b\s*[:=]\s*"
    r"(nan|[-+]?inf(?:inity)?|[-+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:e[-+]?[0-9]+)?)",
    re.IGNORECASE,
)
PROGRESS_GRAD_PATTERN = re.compile(
    r"\b(?:grad(?:ient)?[_ /-]?norm)\b\s*[:=]\s*"
    r"(nan|[-+]?inf(?:inity)?|[-+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:e[-+]?[0-9]+)?)",
    re.IGNORECASE,
)
PROGRESS_THROUGHPUT_PATTERN = re.compile(
    r"\b(?:throughput|samples(?:/|_per_)s(?:ec)?|tokens(?:/|_per_)s(?:ec)?|"
    r"steps(?:/|_per_)s(?:ec)?|it/s)\b\s*[:=]?\s*"
    r"([-+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:e[-+]?[0-9]+)?)",
    re.IGNORECASE,
)
DEFAULT_UNKNOWN_WARNING_PATTERN = re.compile(
    r"\b(?:warning|warn|error|exception|unexpected|retrying|retry)\b", re.IGNORECASE
)
DEFAULT_CHECKPOINT_PATTERN = re.compile(
    r"\b(?:saving|saved|wrote|written)\b[^\n]{0,160}\b(?:model|checkpoint|ckpt)\b",
    re.IGNORECASE,
)
DEFAULT_TRAINING_COMPLETE_PATTERN = re.compile(
    r"\b(?:training|train)\b[^\n]{0,120}\b(?:complete(?:d)?|finished|done)\b|"
    r"\brun_complete_manifest\b",
    re.IGNORECASE,
)
DEFAULT_EVALUATION_COMPLETE_PATTERN = re.compile(
    r"\b(?:evaluation|eval)\b[^\n]{0,120}\b(?:complete(?:d)?|finished|done)\b|"
    r"\bevaluation_complete(?:\.manifest)?\b",
    re.IGNORECASE,
)

EVENT_TYPES = {
    "PROGRESS",
    "MILESTONE",
    "CHECKPOINT",
    "EVAL_STARTED",
    "EVAL_COMPLETED",
    "TRAINING_COMPLETED",
    "KNOWN_WARNING",
    "UNKNOWN_WARNING",
    "INVARIANT_FAILED",
    "PROCESS_FAILED",
    "STALL",
    "ARTIFACT_MISSING",
    "ARTIFACT_INVALID",
    "SCIENTIFIC_REVIEW_REQUIRED",
}
ROUTES = {"RECORD", "LUNA", "SOL"}
LUNA_CLASSIFICATIONS = {"ROUTINE", "WARNING", "FRONTIER_REQUIRED", "UNKNOWN"}
LUNA_ACTIONS = {"IGNORE", "MONITOR", "DETERMINISTIC_ACTION", "ESCALATE"}


class ClusterManagerError(RuntimeError):
    """A concise user-facing cluster-manager failure."""


@dataclass(frozen=True)
class JobRecord:
    job_id: str
    name: str
    state: str
    elapsed: str = ""
    time_left: str = ""
    nodes: str = ""
    location: str = ""
    start: str = ""
    end: str = ""
    exit_code: str = ""
    source: str = "squeue"

    @property
    def normalized_state(self) -> str:
        return normalize_state(self.state)

    @property
    def terminal(self) -> bool:
        return self.normalized_state in TERMINAL_STATES

    @property
    def failed(self) -> bool:
        return self.normalized_state in FAILURE_STATES

    def signature(self) -> dict[str, str]:
        # Elapsed time changes continuously and is intentionally excluded.
        return {
            "state": self.normalized_state,
            "location": self.location,
            "start": self.start,
            "end": self.end,
            "exit_code": self.exit_code,
        }


Runner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_state(state: str) -> str:
    return state.strip().upper().split(maxsplit=1)[0].rstrip("+") if state.strip() else "UNKNOWN"


def validate_job_ids(job_ids: Sequence[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for raw in job_ids:
        for job_id in raw.split(","):
            job_id = job_id.strip()
            if not JOB_ID_RE.fullmatch(job_id):
                raise ClusterManagerError(f"invalid Slurm job id: {job_id!r}")
            if job_id not in seen:
                ordered.append(job_id)
                seen.add(job_id)
    return ordered


def default_runner(args: Sequence[str]) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            list(args),
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise ClusterManagerError(f"required command is unavailable: {args[0]}") from exc


def checked_output(args: Sequence[str], runner: Runner) -> str:
    result = runner(args)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "unknown error").strip().splitlines()
        concise = detail[-1][:500] if detail else "unknown error"
        raise ClusterManagerError(f"{args[0]} failed: {concise}")
    return result.stdout


def parse_squeue(output: str) -> list[JobRecord]:
    jobs: list[JobRecord] = []
    for line in output.splitlines():
        if not line.strip():
            continue
        fields = line.rstrip().split("|", 7)
        if len(fields) != 8:
            raise ClusterManagerError(f"unexpected squeue row with {len(fields)} fields")
        job_id, name, state, elapsed, time_left, nodes, location, start = fields
        jobs.append(
            JobRecord(
                job_id=job_id,
                name=name,
                state=state,
                elapsed=elapsed,
                time_left=time_left,
                nodes=nodes,
                location=location,
                start=start,
                source="squeue",
            )
        )
    return jobs


def parse_sacct(output: str) -> list[JobRecord]:
    jobs: list[JobRecord] = []
    for line in output.splitlines():
        if not line.strip():
            continue
        fields = line.rstrip().split("|", 7)
        if len(fields) != 8:
            raise ClusterManagerError(f"unexpected sacct row with {len(fields)} fields")
        job_id, name, state, elapsed, exit_code, nodes, start, end = fields
        # Allocation rows are sufficient; ignore step rows if a cluster returns them.
        if "." in job_id:
            continue
        jobs.append(
            JobRecord(
                job_id=job_id,
                name=name,
                state=state,
                elapsed=elapsed,
                nodes=nodes,
                location=nodes,
                start=start,
                end=end,
                exit_code=exit_code,
                source="sacct",
            )
        )
    return jobs


def query_jobs(
    job_ids: Sequence[str],
    *,
    user: str,
    runner: Runner = default_runner,
) -> tuple[list[JobRecord], list[str]]:
    warnings: list[str] = []
    squeue_format = "%i|%j|%T|%M|%L|%D|%R|%S"
    if job_ids:
        command = ["squeue", "-h", "-j", ",".join(job_ids), "-o", squeue_format]
    else:
        command = ["squeue", "-h", "-u", user, "-o", squeue_format]

    try:
        active_output = checked_output(command, runner)
    except ClusterManagerError as exc:
        # Slurm reports purged/completed IDs as an squeue error. They are still
        # valid accounting queries, so continue to sacct instead of failing.
        if job_ids and "invalid job id" in str(exc).lower():
            active_output = ""
        else:
            raise
    active = parse_squeue(active_output)
    if not job_ids:
        return sorted(active, key=lambda job: job.job_id), warnings

    active_by_id = {job.job_id: job for job in active}
    missing = [job_id for job_id in job_ids if job_id not in active_by_id]
    accounting_by_id: dict[str, JobRecord] = {}
    if missing:
        sacct_command = [
            "sacct",
            "-X",
            "-n",
            "-P",
            "-j",
            ",".join(missing),
            "--format=JobIDRaw,JobName,State,Elapsed,ExitCode,NodeList,Start,End",
        ]
        try:
            accounting_by_id = {job.job_id: job for job in parse_sacct(checked_output(sacct_command, runner))}
        except ClusterManagerError as exc:
            warnings.append(str(exc))

    jobs: list[JobRecord] = []
    for job_id in job_ids:
        job = active_by_id.get(job_id) or accounting_by_id.get(job_id)
        if job is None:
            job = JobRecord(job_id=job_id, name="", state="UNKNOWN", source="none")
        jobs.append(job)
    return jobs, warnings


def default_state_file(cwd: Path) -> Path:
    explicit_dir = os.environ.get("CLUSTER_MANAGER_STATE_DIR")
    if explicit_dir:
        base = Path(explicit_dir).expanduser()
    elif os.environ.get("XDG_CACHE_HOME"):
        base = Path(os.environ["XDG_CACHE_HOME"]).expanduser() / "cluster-manager"
    else:
        base = Path.home() / ".cache" / "cluster-manager"
    namespace = hashlib.sha256(str(cwd.resolve()).encode()).hexdigest()[:12]
    return base / f"{namespace}.json"


def load_state(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text())
    except FileNotFoundError:
        return {"schema": SCHEMA_VERSION, "jobs": {}}
    except (OSError, json.JSONDecodeError) as exc:
        raise ClusterManagerError(f"cannot read state file {path}: {exc}") from exc
    if payload.get("schema") not in {SCHEMA_VERSION, MONITOR_STATE_SCHEMA} or not isinstance(
        payload.get("jobs"), dict
    ):
        return {"schema": SCHEMA_VERSION, "jobs": {}}
    return payload


def atomic_write_json(path: Path, value: dict[str, Any], *, maximum_bytes: int) -> None:
    encoded = (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"
    ).encode("utf-8")
    if len(encoded) > maximum_bytes:
        raise ClusterManagerError(
            f"refusing to write oversized JSON state {path}: {len(encoded)} bytes"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError as exc:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise ClusterManagerError(f"cannot write state file {path}: {exc}") from exc


def save_state(path: Path, state: dict[str, Any]) -> None:
    atomic_write_json(path, state, maximum_bytes=MAX_MONITOR_STATE_BYTES)


@contextlib.contextmanager
def monitor_state_lock(path: Path) -> Iterator[None]:
    """Prevent duplicate event watchers from racing one durable cursor."""

    lock_path = path.with_name(f"{path.name}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with lock_path.open("a+b") as handle:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ClusterManagerError(
                    f"monitor state is already owned by another process: {path}"
                ) from exc
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except ClusterManagerError:
        raise
    except OSError as exc:
        raise ClusterManagerError(f"cannot lock monitor state {path}: {exc}") from exc


def parse_log_bindings(values: Sequence[str], job_ids: Sequence[str], cwd: Path) -> dict[str, Path]:
    bindings: dict[str, Path] = {}
    for value in values:
        if "=" in value:
            job_id, raw_path = value.split("=", 1)
            validate_job_ids([job_id])
        elif len(job_ids) == 1:
            job_id, raw_path = job_ids[0], value
        else:
            raise ClusterManagerError("--log requires JOB_ID=PATH when multiple jobs are monitored")
        path = Path(raw_path).expanduser()
        bindings[job_id] = path if path.is_absolute() else cwd / path
    return bindings


def infer_log_from_scontrol(job_id: str, runner: Runner) -> Path | None:
    try:
        output = checked_output(["scontrol", "show", "job", "-o", job_id], runner)
    except ClusterManagerError:
        return None
    fields: dict[str, str] = {}
    for token in output.strip().split():
        if "=" in token:
            key, value = token.split("=", 1)
            fields[key] = value
    stdout = fields.get("StdOut")
    if not stdout or stdout in {"(null)", "NONE"}:
        return None
    path = Path(stdout).expanduser()
    if not path.is_absolute() and fields.get("WorkDir"):
        path = Path(fields["WorkDir"]) / path
    return path


def find_log_path(
    job: JobRecord,
    *,
    explicit_logs: dict[str, Path],
    cwd: Path,
    runner: Runner,
    auto_log: bool,
) -> Path | None:
    if job.job_id in explicit_logs:
        return explicit_logs[job.job_id]
    if not auto_log:
        return None

    candidates: set[Path] = set()
    for pattern in (f"*{job.job_id}*.log", f"slurm-{job.job_id}.out"):
        candidates.update(path for path in cwd.glob(pattern) if path.is_file())
    if candidates:
        return max(candidates, key=lambda path: path.stat().st_mtime_ns)
    return infer_log_from_scontrol(job.job_id, runner)


def compile_patterns(defaults: Sequence[str], additions: Sequence[str]) -> list[re.Pattern[str]]:
    patterns: list[re.Pattern[str]] = []
    for expression in [*defaults, *additions]:
        try:
            patterns.append(re.compile(expression, re.IGNORECASE))
        except re.error as exc:
            raise ClusterManagerError(f"invalid regular expression {expression!r}: {exc}") from exc
    return patterns


def matching_lines(
    lines: Sequence[str],
    patterns: Sequence[re.Pattern[str]],
    *,
    limit: int = 3,
) -> list[str]:
    matches: list[str] = []
    for line in lines:
        if not any(pattern.search(line) for pattern in patterns):
            continue
        cleaned = line.strip()
        if len(cleaned) > 300:
            cleaned = f"{cleaned[:210]}…{cleaned[-89:]}"
        matches.append(cleaned)
    return matches[-limit:]


def display_path(path: Path, cwd: Path) -> str:
    try:
        return str(path.resolve().relative_to(cwd.resolve()))
    except ValueError:
        return str(path)


def _complete_log_lines(
    raw: bytes,
    *,
    previous_partial_hex: str,
    starts_mid_line: bool,
) -> tuple[list[str], str]:
    try:
        prefix = bytes.fromhex(previous_partial_hex) if previous_partial_hex else b""
    except ValueError:
        prefix = b""
    combined = prefix + raw
    pieces = combined.split(b"\n")
    if combined.endswith(b"\n"):
        complete, partial = pieces[:-1], b""
    else:
        complete, partial = pieces[:-1], pieces[-1]
    if starts_mid_line and not prefix and complete:
        complete = complete[1:]
    lines = [item.rstrip(b"\r").decode("utf-8", errors="replace") for item in complete]
    # A pathological application that never writes newlines must not make the
    # compact state grow without bound. Preserve the newest suffix and surface
    # the truncation through the scan result.
    if len(partial) > MAX_EVENT_EVIDENCE_CHARS:
        partial = partial[-MAX_EVENT_EVIDENCE_CHARS:]
    return lines, partial.hex()


def read_log_delta(
    path: Path,
    *,
    previous: dict[str, Any],
    cwd: Path,
    max_bytes: int,
    now_epoch: float | None = None,
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    """Read only bytes not represented by the durable cursor.

    The cursor records path, inode, byte offset, a partial trailing line, and
    growth timestamps.  Inode changes and truncation reset the cursor safely;
    a late first attachment samples only a bounded tail instead of rereading a
    multi-gigabyte log.
    """

    now_epoch = time.time() if now_epoch is None else now_epoch
    shown_path = display_path(path, cwd)
    result: dict[str, Any] = {"path": shown_path}
    now = datetime.fromtimestamp(now_epoch, timezone.utc).replace(microsecond=0).isoformat()
    if not path.exists():
        result["missing"] = True
        return result, {
            "path": str(path),
            "offset": 0,
            "size": 0,
            "inode": None,
            "mtime_ns": 0,
            "partial_hex": "",
            "last_seen_at": now,
            "last_growth_at": previous.get("last_growth_at", ""),
            "last_growth_at_epoch": previous.get("last_growth_at_epoch"),
        }, []
    if not path.is_file():
        result["not_file"] = True
        return result, {
            "path": str(path),
            "offset": 0,
            "size": 0,
            "inode": None,
            "mtime_ns": 0,
            "partial_hex": "",
            "last_seen_at": now,
            "last_growth_at": previous.get("last_growth_at", ""),
            "last_growth_at_epoch": previous.get("last_growth_at_epoch"),
        }, []

    try:
        stat = path.stat()
    except OSError as exc:
        raise ClusterManagerError(f"cannot stat log {path}: {exc}") from exc
    same_path = previous.get("path") == str(path)
    previous_inode = previous.get("inode")
    old_offset = int(previous.get("offset", previous.get("size", 0))) if same_path else 0
    reset = False
    if same_path and previous_inode is not None and int(previous_inode) != int(stat.st_ino):
        result["rotated"] = True
        reset = True
    elif same_path and stat.st_size < old_offset:
        result["truncated"] = True
        reset = True
    elif previous and not same_path:
        result["rebound"] = True
        reset = True

    if reset:
        old_offset = 0
    first_attachment = not previous or not same_path
    start = old_offset
    if first_attachment and stat.st_size > max_bytes:
        start = stat.st_size - max_bytes
        result["initial_tail"] = True
        result["initial_skipped_bytes"] = start
    available = max(0, stat.st_size - start)
    read_size = min(max_bytes, available)
    starts_mid_line = False
    try:
        with path.open("rb") as handle:
            if start > 0:
                handle.seek(start - 1)
                starts_mid_line = handle.read(1) != b"\n"
            handle.seek(start)
            raw = handle.read(read_size)
    except OSError as exc:
        raise ClusterManagerError(f"cannot read log {path}: {exc}") from exc

    prior_partial = "" if reset or start != old_offset else str(previous.get("partial_hex", ""))
    lines, partial_hex = _complete_log_lines(
        raw,
        previous_partial_hex=prior_partial,
        starts_mid_line=starts_mid_line,
    )
    next_offset = start + len(raw)
    result.update(
        {
            "size": stat.st_size,
            "inode": stat.st_ino,
            "offset": next_offset,
            "new_bytes": len(raw),
        }
    )
    if next_offset < stat.st_size:
        result["backlog_bytes"] = stat.st_size - next_offset
    if partial_hex:
        result["partial_line_bytes"] = len(bytes.fromhex(partial_hex))
    next_state = {
        **{
            key: value
            for key, value in previous.items()
            if key
            not in {
                "path",
                "offset",
                "size",
                "inode",
                "mtime_ns",
                "partial_hex",
                "last_seen_at",
                "last_growth_at",
                "last_growth_at_epoch",
            }
        },
        "path": str(path),
        "offset": next_offset,
        "size": stat.st_size,
        "inode": stat.st_ino,
        "mtime_ns": stat.st_mtime_ns,
        "partial_hex": partial_hex,
        "last_seen_at": now,
        "last_growth_at": now if raw else previous.get("last_growth_at", now),
        "last_growth_at_epoch": now_epoch if raw else previous.get("last_growth_at_epoch"),
    }
    return result, next_state, lines


def scan_log(
    path: Path,
    *,
    previous: dict[str, Any],
    cwd: Path,
    max_bytes: int,
    milestone_patterns: Sequence[re.Pattern[str]],
    error_patterns: Sequence[re.Pattern[str]],
    include_lines: bool = False,
    now_epoch: float | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    result, next_state, lines = read_log_delta(
        path,
        previous=previous,
        cwd=cwd,
        max_bytes=max_bytes,
        now_epoch=now_epoch,
    )
    if result.get("missing") or result.get("not_file"):
        return result, next_state
    milestone_candidates = matching_lines(lines, milestone_patterns)
    error_candidates = matching_lines(lines, error_patterns)
    observed_steps = [
        int(match.group(1)) for line in lines for match in PROGRESS_STEP_PATTERN.finditer(line)
    ]
    seen_milestones = list(previous.get("seen_milestones", []))
    seen_errors = list(previous.get("seen_errors", []))
    milestone_seen_set = set(seen_milestones)
    error_seen_set = set(seen_errors)
    milestones = [line for line in milestone_candidates if line not in milestone_seen_set]
    errors = [line for line in error_candidates if line not in error_seen_set]
    if milestones:
        result["milestones"] = milestones
    if errors:
        result["errors"] = errors
    if observed_steps and not include_lines:
        previous_step = previous.get("last_step")
        last_step = max(observed_steps)
        if isinstance(previous_step, int):
            last_step = max(last_step, previous_step)
        result["progress"] = {"last_step": last_step}
        next_state["last_step"] = last_step
    if include_lines:
        result["_lines"] = lines
    next_state.update(
        {
            "seen_milestones": [*seen_milestones, *milestone_candidates][-64:],
            "seen_errors": [*seen_errors, *error_candidates][-64:],
        }
    )
    return result, next_state


def build_report(
    job_ids: Sequence[str],
    *,
    user: str,
    cwd: Path,
    state: dict[str, Any],
    scan_logs: bool,
    explicit_logs: dict[str, Path],
    auto_log: bool,
    max_log_bytes: int,
    milestone_patterns: Sequence[re.Pattern[str]],
    error_patterns: Sequence[re.Pattern[str]],
    runner: Runner = default_runner,
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        jobs, warnings = query_jobs(job_ids, user=user, runner=runner)
    except ClusterManagerError as exc:
        # A controller outage must not disable bounded monitoring of logs whose
        # paths were supplied explicitly. Preserve UNKNOWN scheduler state and
        # surface the RPC failure as a warning; terminal-state inference stays
        # disabled until Slurm recovers. Without one explicit log per requested
        # job, fail as before rather than silently degrading observability.
        if not scan_logs or any(job_id not in explicit_logs for job_id in job_ids):
            raise
        warnings = [
            str(exc),
            "scheduler unavailable; monitoring explicitly bound logs only",
        ]
        jobs = [
            JobRecord(job_id=job_id, name="", state="UNKNOWN", source="log_only")
            for job_id in job_ids
        ]
    previous_jobs = state.get("jobs", {})
    next_jobs = dict(previous_jobs)
    output_jobs: list[dict[str, Any]] = []

    for job in jobs:
        previous = previous_jobs.get(job.job_id, {})
        previous_signature = previous.get("signature")
        signature = job.signature()
        state_changed = previous_signature is not None and previous_signature != signature
        first_seen = previous_signature is None
        log_result: dict[str, Any] | None = None
        log_state = previous.get("log", {})

        if scan_logs:
            log_path = find_log_path(
                job,
                explicit_logs=explicit_logs,
                cwd=cwd,
                runner=runner,
                auto_log=auto_log,
            )
            if log_path is not None:
                log_result, log_state = scan_log(
                    log_path,
                    previous=log_state,
                    cwd=cwd,
                    max_bytes=max_log_bytes,
                    milestone_patterns=milestone_patterns,
                    error_patterns=error_patterns,
                )

        new_milestones = list((log_result or {}).get("milestones", []))
        new_errors = list((log_result or {}).get("errors", []))
        anomaly = job.failed or bool(new_errors)
        changed = first_seen or state_changed or bool(new_milestones) or bool(new_errors)

        item = asdict(job)
        item.update(
            {
                "state": job.normalized_state,
                "terminal": job.terminal,
                "anomaly": anomaly,
                "changed": changed,
            }
        )
        if state_changed:
            item["previous_state"] = previous_signature.get("state", "UNKNOWN")
        if log_result is not None:
            item["log"] = log_result
        output_jobs.append(compact(item))
        next_jobs[job.job_id] = {"signature": signature, "log": log_state}

    counts = Counter(job["state"] for job in output_jobs)
    report: dict[str, Any] = {
        "schema": SCHEMA_VERSION,
        "checked_at": utc_now(),
        "changed": any(job["changed"] for job in output_jobs),
        "anomaly": any(job["anomaly"] for job in output_jobs),
        "jobs": output_jobs,
        "summary": {
            "total": len(output_jobs),
            "changed": sum(bool(job["changed"]) for job in output_jobs),
            "terminal": sum(bool(job["terminal"]) for job in output_jobs),
            "anomalies": sum(bool(job["anomaly"]) for job in output_jobs),
            "states": dict(sorted(counts.items())),
        },
    }
    if warnings:
        report["warnings"] = warnings
    next_state = {"schema": SCHEMA_VERSION, "jobs": next_jobs}
    return report, next_state


def _require_monitor_text(value: Any, field: str, *, maximum: int = 2_048) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ClusterManagerError(f"{field} must be a non-empty string")
    cleaned = value.strip()
    if len(cleaned) > maximum:
        raise ClusterManagerError(f"{field} exceeds {maximum} characters")
    return cleaned


def _regex_list(value: Any, field: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or len(value) > 128:
        raise ClusterManagerError(f"{field} must be a list with at most 128 expressions")
    expressions: list[str] = []
    for index, item in enumerate(value):
        expression = _require_monitor_text(item, f"{field}[{index}]", maximum=2_000)
        try:
            re.compile(expression, re.IGNORECASE)
        except re.error as exc:
            raise ClusterManagerError(f"invalid {field}[{index}] regular expression: {exc}") from exc
        expressions.append(expression)
    return expressions


def validate_monitor_spec(value: dict[str, Any], *, cwd: Path) -> dict[str, Any]:
    if value.get("schema", MONITOR_SPEC_SCHEMA) != MONITOR_SPEC_SCHEMA:
        raise ClusterManagerError("unsupported monitor specification schema")
    experiment_id = _require_monitor_text(value.get("experiment_id"), "experiment_id", maximum=128)
    phase = str(value.get("phase", "TRAINING")).strip().upper()
    if phase not in {"TRAINING", "EVALUATION", "REDUCTION", "OTHER"}:
        raise ClusterManagerError("monitor phase must be TRAINING, EVALUATION, REDUCTION, or OTHER")
    target_step = value.get("target_step")
    if target_step is not None and (isinstance(target_step, bool) or not isinstance(target_step, int) or target_step < 0):
        raise ClusterManagerError("target_step must be a non-negative integer")

    job_ids = validate_job_ids([str(item) for item in value.get("job_ids", [])])
    log_bindings_raw = value.get("log_bindings", {})
    if not isinstance(log_bindings_raw, dict):
        raise ClusterManagerError("log_bindings must be an object keyed by job ID")
    log_bindings: dict[str, str] = {}
    for raw_job_id, raw_path in log_bindings_raw.items():
        job_id = validate_job_ids([str(raw_job_id)])[0]
        log_bindings[job_id] = _require_monitor_text(raw_path, f"log_bindings.{job_id}")

    thresholds_raw = value.get("thresholds", {})
    if not isinstance(thresholds_raw, dict):
        raise ClusterManagerError("thresholds must be an object")
    thresholds: dict[str, float | int] = {
        "stall_seconds": 1_800.0,
        "log_stall_seconds": 1_800.0,
        "scheduler_unknown_seconds": 300.0,
        "dedupe_window_seconds": 21_600.0,
        "metric_window": 31,
        "minimum_metric_samples": 7,
        "consecutive_violations": 3,
        "loss_mad_z": 12.0,
        "gradient_mad_z": 12.0,
        "throughput_ratio": 0.10,
        "step_regression_tolerance": 1,
        "luna_min_confidence": 0.80,
    }
    for key, default in list(thresholds.items()):
        raw = thresholds_raw.get(key, default)
        if isinstance(default, int):
            if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
                raise ClusterManagerError(f"thresholds.{key} must be a non-negative integer")
        else:
            if isinstance(raw, bool) or not isinstance(raw, (int, float)) or not math.isfinite(float(raw)) or float(raw) < 0:
                raise ClusterManagerError(f"thresholds.{key} must be a finite non-negative number")
        thresholds[key] = raw
    if int(thresholds["metric_window"]) < 5:
        raise ClusterManagerError("thresholds.metric_window must be at least 5")
    if int(thresholds["minimum_metric_samples"]) < 3:
        raise ClusterManagerError("thresholds.minimum_metric_samples must be at least 3")
    if int(thresholds["consecutive_violations"]) < 1:
        raise ClusterManagerError("thresholds.consecutive_violations must be at least 1")
    if not 0 <= float(thresholds["throughput_ratio"]) <= 1:
        raise ClusterManagerError("thresholds.throughput_ratio must be between 0 and 1")
    if not 0 <= float(thresholds["luna_min_confidence"]) <= 1:
        raise ClusterManagerError("thresholds.luna_min_confidence must be between 0 and 1")

    artifacts_raw = value.get("artifacts", [])
    if not isinstance(artifacts_raw, list) or len(artifacts_raw) > 128:
        raise ClusterManagerError("artifacts must be a list with at most 128 entries")
    artifacts: list[dict[str, Any]] = []
    for index, raw in enumerate(artifacts_raw):
        if not isinstance(raw, dict):
            raise ClusterManagerError(f"artifacts[{index}] must be an object")
        kind = str(raw.get("kind", "artifact")).strip().lower()
        if kind not in {"artifact", "checkpoint", "evaluation_complete", "training_complete"}:
            raise ClusterManagerError(f"artifacts[{index}].kind is invalid")
        required_when = str(raw.get("required_when", "never")).strip().lower()
        if required_when not in {"never", "target_step", "terminal"}:
            raise ClusterManagerError(f"artifacts[{index}].required_when is invalid")
        expected_sha256 = str(raw.get("sha256", "")).strip().lower()
        if expected_sha256 and not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
            raise ClusterManagerError(f"artifacts[{index}].sha256 must contain 64 hexadecimal characters")
        artifacts.append(
            {
                "path": _require_monitor_text(raw.get("path"), f"artifacts[{index}].path"),
                "kind": kind,
                "wake_on_create": bool(raw.get("wake_on_create", kind != "artifact")),
                "required_when": required_when,
                "sha256": expected_sha256,
            }
        )

    processes_raw = value.get("processes", [])
    if not isinstance(processes_raw, list) or len(processes_raw) > 128:
        raise ClusterManagerError("processes must be a list with at most 128 entries")
    processes: list[dict[str, Any]] = []
    for index, raw in enumerate(processes_raw):
        if not isinstance(raw, dict) or isinstance(raw.get("pid"), bool) or not isinstance(raw.get("pid"), int):
            raise ClusterManagerError(f"processes[{index}].pid must be an integer")
        if raw["pid"] <= 0:
            raise ClusterManagerError(f"processes[{index}].pid must be positive")
        processes.append(
            {
                "pid": raw["pid"],
                "name": str(raw.get("name", raw["pid"]))[:256],
                "required": bool(raw.get("required", True)),
            }
        )

    wake_conditions_raw = value.get("wake_conditions", [])
    if not isinstance(wake_conditions_raw, list) or len(wake_conditions_raw) > 128:
        raise ClusterManagerError("wake_conditions must be a list with at most 128 entries")
    wake_conditions = [
        _require_monitor_text(item, f"wake_conditions[{index}]", maximum=1_000)
        for index, item in enumerate(wake_conditions_raw)
    ]
    packet_bytes = value.get("luna_max_input_bytes", DEFAULT_LUNA_PACKET_BYTES)
    if (
        isinstance(packet_bytes, bool)
        or not isinstance(packet_bytes, int)
        or not MIN_LUNA_PACKET_BYTES <= packet_bytes <= MAX_LUNA_PACKET_BYTES
    ):
        raise ClusterManagerError(
            f"luna_max_input_bytes must be between {MIN_LUNA_PACKET_BYTES} and "
            f"{MAX_LUNA_PACKET_BYTES}"
        )

    normalized = {
        "schema": MONITOR_SPEC_SCHEMA,
        "experiment_id": experiment_id,
        "phase": phase,
        "job_ids": job_ids,
        "log_bindings": log_bindings,
        "target_step": target_step,
        "wake_conditions": wake_conditions,
        "next_scientific_action": str(value.get("next_scientific_action", ""))[:2_000],
        "thresholds": thresholds,
        "artifacts": artifacts,
        "processes": processes,
        "known_warning_regex": _regex_list(value.get("known_warning_regex"), "known_warning_regex"),
        "unknown_warning_regex": _regex_list(value.get("unknown_warning_regex"), "unknown_warning_regex"),
        "training_complete_regex": _regex_list(value.get("training_complete_regex"), "training_complete_regex"),
        "evaluation_complete_regex": _regex_list(value.get("evaluation_complete_regex"), "evaluation_complete_regex"),
        "scientific_event_regex": _regex_list(value.get("scientific_event_regex"), "scientific_event_regex"),
        "luna_max_input_bytes": packet_bytes,
        "event_log_path": str(value.get("event_log_path", ""))[:2_048],
        "wake_file": str(value.get("wake_file", ""))[:2_048],
    }
    return normalized


def load_monitor_spec(path: Path, *, cwd: Path) -> dict[str, Any]:
    resolved = path.expanduser()
    resolved = resolved if resolved.is_absolute() else cwd / resolved
    try:
        size = resolved.stat().st_size
        if size > MAX_MONITOR_SPEC_BYTES:
            raise ClusterManagerError(f"monitor specification exceeds {MAX_MONITOR_SPEC_BYTES} bytes")
        value = json.loads(resolved.read_text(encoding="utf-8"))
    except ClusterManagerError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ClusterManagerError(f"cannot read monitor specification {resolved}: {exc}") from exc
    if not isinstance(value, dict):
        raise ClusterManagerError("monitor specification must contain one JSON object")
    return validate_monitor_spec(value, cwd=cwd)


def monitor_spec_fingerprint(spec: dict[str, Any]) -> str:
    encoded = json.dumps(
        spec, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def telemetry_template() -> dict[str, int]:
    return {
        "monitor_poll_count": 0,
        "bytes_read_incrementally": 0,
        "events_emitted": 0,
        "events_deduplicated": 0,
        "luna_invocations": 0,
        "luna_input_tokens": 0,
        "luna_output_tokens": 0,
        "sol_wakeups": 0,
        "frontier_no_change_wakeups": 0,
        "full_log_reads": 0,
        "handoff_bytes": 0,
    }


def initial_monitor_state(spec: dict[str, Any], *, now_epoch: float | None = None) -> dict[str, Any]:
    now_epoch = time.time() if now_epoch is None else now_epoch
    return {
        "schema": MONITOR_STATE_SCHEMA,
        "experiment_id": spec["experiment_id"],
        "spec_sha256": monitor_spec_fingerprint(spec),
        "phase": spec["phase"],
        "scheduler_status": "UNKNOWN",
        "last_step": None,
        "target_step": spec.get("target_step"),
        "last_checkpoint": None,
        "last_loss": None,
        "warnings": [],
        "pending_evaluations": [],
        "wake_conditions": spec.get("wake_conditions", []),
        "jobs": {},
        "artifacts": {},
        "processes": {},
        "dedupe": {},
        "once_events": {},
        "pending_luna": None,
        "last_luna_resolution": None,
        "last_wake": None,
        "started_at": datetime.fromtimestamp(now_epoch, timezone.utc).replace(microsecond=0).isoformat(),
        "started_at_epoch": now_epoch,
        "updated_at": datetime.fromtimestamp(now_epoch, timezone.utc).replace(microsecond=0).isoformat(),
        "telemetry": telemetry_template(),
    }


def prepare_monitor_state(
    state: dict[str, Any], spec: dict[str, Any], *, now_epoch: float
) -> dict[str, Any]:
    if state.get("schema") != MONITOR_STATE_SCHEMA or not state.get("experiment_id"):
        return initial_monitor_state(spec, now_epoch=now_epoch)
    if state.get("experiment_id") != spec["experiment_id"]:
        raise ClusterManagerError("monitor state belongs to a different experiment")
    expected = monitor_spec_fingerprint(spec)
    if state.get("spec_sha256") != expected:
        raise ClusterManagerError(
            "monitor specification changed after arming; create a new state file or re-arm explicitly"
        )
    prepared = dict(state)
    prepared["jobs"] = dict(state.get("jobs", {}))
    prepared["artifacts"] = dict(state.get("artifacts", {}))
    prepared["processes"] = dict(state.get("processes", {}))
    prepared["dedupe"] = dict(state.get("dedupe", {}))
    prepared["once_events"] = dict(state.get("once_events", {}))
    telemetry = telemetry_template()
    telemetry.update(
        {
            key: int(value)
            for key, value in state.get("telemetry", {}).items()
            if key in telemetry and isinstance(value, (int, float)) and not isinstance(value, bool)
        }
    )
    prepared["telemetry"] = telemetry
    return prepared


def route_for_event(event: dict[str, Any]) -> str:
    if event.get("requires_sol"):
        return "SOL"
    if event.get("requires_luna"):
        return "LUNA"
    event_type = event.get("event")
    if event_type in {
        "MILESTONE",
        "EVAL_COMPLETED",
        "TRAINING_COMPLETED",
        "INVARIANT_FAILED",
        "PROCESS_FAILED",
        "STALL",
        "ARTIFACT_MISSING",
        "ARTIFACT_INVALID",
        "SCIENTIFIC_REVIEW_REQUIRED",
    }:
        return "SOL"
    if event_type == "UNKNOWN_WARNING":
        return "LUNA"
    return "RECORD"


def make_event(
    event_type: str,
    *,
    experiment_id: str,
    source: str,
    severity: str,
    dedupe_key: str,
    evidence_ref: str = "",
    evidence: str = "",
    data: dict[str, Any] | None = None,
    requires_luna: bool = False,
    requires_sol: bool = False,
    once: bool = False,
    now_epoch: float | None = None,
) -> dict[str, Any]:
    if event_type not in EVENT_TYPES:
        raise ClusterManagerError(f"unknown monitor event type: {event_type}")
    if severity not in {"info", "warning", "critical"}:
        raise ClusterManagerError(f"unknown monitor event severity: {severity}")
    now_epoch = time.time() if now_epoch is None else now_epoch
    stable = hashlib.sha256(
        f"{experiment_id}\0{event_type}\0{source}\0{dedupe_key}".encode("utf-8")
    ).hexdigest()[:24]
    event = {
        "schema": 1,
        "id": stable,
        "timestamp": datetime.fromtimestamp(now_epoch, timezone.utc).replace(microsecond=0).isoformat(),
        "experiment_id": experiment_id,
        "event": event_type,
        "source": source,
        "severity": severity,
        "evidence_ref": evidence_ref,
        "dedupe_key": dedupe_key,
        "requires_luna": requires_luna,
        "requires_sol": requires_sol,
        "once": once,
    }
    if evidence:
        event["evidence"] = evidence[:MAX_EVENT_EVIDENCE_CHARS]
    if data:
        event["data"] = data
    event["route"] = route_for_event(event)
    return compact(event)


def accept_event(
    event: dict[str, Any],
    *,
    state: dict[str, Any],
    spec: dict[str, Any],
    now_epoch: float,
) -> bool:
    dedupe = state["dedupe"]
    once_events = state["once_events"]
    key = str(event["dedupe_key"])
    if event.get("once") and key in once_events:
        state["telemetry"]["events_deduplicated"] += 1
        return False
    previous = dedupe.get(key)
    window = float(spec["thresholds"]["dedupe_window_seconds"])
    if previous and (
        event.get("once") or now_epoch - float(previous.get("last_emitted_epoch", 0)) < window
    ):
        previous["count"] = int(previous.get("count", 1)) + 1
        previous["last_seen_epoch"] = now_epoch
        state["telemetry"]["events_deduplicated"] += 1
        return False
    dedupe[key] = {
        "event_id": event["id"],
        "last_emitted_epoch": now_epoch,
        "last_seen_epoch": now_epoch,
        "count": 1,
    }
    if event.get("once"):
        if len(once_events) >= MAX_ONCE_ENTRIES:
            raise ClusterManagerError(
                "monitor one-shot event registry is full; inspect the specification before continuing"
            )
        once_events[key] = {"event_id": event["id"], "emitted_at_epoch": now_epoch}
    if len(dedupe) > MAX_DEDUPE_ENTRIES:
        oldest = sorted(
            dedupe,
            key=lambda item: float(dedupe[item].get("last_seen_epoch", 0)),
        )[: len(dedupe) - MAX_DEDUPE_ENTRIES]
        for item in oldest:
            dedupe.pop(item, None)
    state["telemetry"]["events_emitted"] += 1
    return True


def _number(raw: str) -> float:
    cleaned = raw.strip().lower()
    if cleaned in {"nan", "+nan", "-nan"}:
        return float("nan")
    if cleaned in {"inf", "+inf", "infinity", "+infinity"}:
        return float("inf")
    if cleaned in {"-inf", "-infinity"}:
        return float("-inf")
    return float(cleaned)


def _bounded_line(line: str, maximum: int = 500) -> str:
    cleaned = line.strip()
    if len(cleaned) <= maximum:
        return cleaned
    head = max(1, maximum * 2 // 3)
    return f"{cleaned[:head]}…{cleaned[-(maximum - head - 1):]}"


def _evidence_window(previous_lines: Sequence[str], lines: Sequence[str], index: int) -> str:
    prefix = previous_lines[-2:]
    prefix_length = len(prefix)
    shifted = prefix_length + index
    start = max(0, shifted - 1)
    stop = min(prefix_length + len(lines), shifted + 2)
    selected = [
        prefix[position]
        if position < prefix_length
        else lines[position - prefix_length]
        for position in range(start, stop)
    ]
    return "\n".join(_bounded_line(item, 1_000) for item in selected)[:MAX_EVENT_EVIDENCE_CHARS]


def _series_summary(values: Sequence[float]) -> dict[str, Any]:
    finite = [value for value in values if math.isfinite(value)]
    if not values:
        return {}
    result: dict[str, Any] = {
        "count": len(values),
        "finite": len(finite),
        "nonfinite": len(values) - len(finite),
    }
    if finite:
        result.update(
            {
                "min": min(finite),
                "median": median(finite),
                "max": max(finite),
                "last": finite[-1],
            }
        )
    return result


def _robust_upper_violation(
    history: Sequence[float], value: float, *, minimum: int, z_limit: float
) -> bool:
    finite = [item for item in history if math.isfinite(item)]
    if len(finite) < minimum or not math.isfinite(value):
        return False
    center = median(finite)
    deviations = [abs(item - center) for item in finite]
    mad = median(deviations)
    if mad > 0:
        return value > center + z_limit * 1.4826 * mad
    return center > 0 and value > center * 2


def _robust_lower_violation(
    history: Sequence[float], value: float, *, minimum: int, ratio: float
) -> bool:
    finite = [item for item in history if math.isfinite(item) and item >= 0]
    if len(finite) < minimum or not math.isfinite(value):
        return False
    center = median(finite)
    return center > 0 and value < center * ratio


def _patterns(expressions: Sequence[str]) -> list[re.Pattern[str]]:
    return [re.compile(item, re.IGNORECASE) for item in expressions]


def reset_log_semantic_window(log_state: dict[str, Any], *, now_epoch: float) -> None:
    """Reset rolling statistics without discarding the durable byte cursor.

    A rotated/truncated log or restarted scheduler allocation starts a new
    metric generation. The previous step is retained so a genuine step reset
    remains detectable, while noisy-metric baselines and debounce counters are
    warmed from the new generation.
    """

    for key in (
        "loss_history",
        "gradient_history",
        "throughput_history",
        "loss_violation_polls",
        "gradient_violation_polls",
        "throughput_violation_polls",
        "loss_violation_samples",
        "gradient_violation_samples",
        "throughput_violation_samples",
        "step_regression_polls",
        "last_loss",
        "last_gradient_norm",
        "last_throughput",
        "context_tail",
    ):
        log_state.pop(key, None)
    log_state["last_step_at_epoch"] = now_epoch
    log_state["semantic_generation"] = int(log_state.get("semantic_generation", 0)) + 1


def parse_log_events(
    *,
    lines: Sequence[str],
    log_result: dict[str, Any],
    log_state: dict[str, Any],
    spec: dict[str, Any],
    milestone_patterns: Sequence[re.Pattern[str]],
    error_patterns: Sequence[re.Pattern[str]],
    now_epoch: float,
) -> list[dict[str, Any]]:
    experiment_id = spec["experiment_id"]
    source = f"log:{log_result['path']}"
    byte_ref = (
        f"{log_result['path']}#bytes={max(0, int(log_result.get('offset', 0)) - int(log_result.get('new_bytes', 0)))}"
        f"-{int(log_result.get('offset', 0))}"
    )
    previous_context = list(log_state.get("context_tail", []))
    known_warning_patterns = _patterns(spec["known_warning_regex"])
    unknown_warning_patterns = _patterns(spec["unknown_warning_regex"])
    training_complete_patterns = [DEFAULT_TRAINING_COMPLETE_PATTERN, *_patterns(spec["training_complete_regex"])]
    evaluation_complete_patterns = [DEFAULT_EVALUATION_COMPLETE_PATTERN, *_patterns(spec["evaluation_complete_regex"])]
    scientific_patterns = _patterns(spec["scientific_event_regex"])
    events: list[dict[str, Any]] = []
    event_overflow = 0

    def record_event(event: dict[str, Any]) -> None:
        nonlocal event_overflow
        if len(events) < MAX_PARSED_EVENTS_PER_LOG:
            events.append(event)
        else:
            event_overflow += 1

    prior_step = log_state.get("last_step")
    prior_step = int(prior_step) if isinstance(prior_step, int) else None
    observed_steps: list[int] = []
    new_losses: list[float] = []
    new_gradients: list[float] = []
    new_throughputs: list[float] = []
    matched_error_indexes: set[int] = set()
    matched_known_indexes: set[int] = set()
    categorized_indexes: set[int] = set()

    for index, line in enumerate(lines):
        observed_steps.extend(int(match.group(1)) for match in PROGRESS_STEP_PATTERN.finditer(line))
        new_losses.extend(_number(match.group(1)) for match in PROGRESS_LOSS_PATTERN.finditer(line))
        new_gradients.extend(_number(match.group(1)) for match in PROGRESS_GRAD_PATTERN.finditer(line))
        new_throughputs.extend(_number(match.group(1)) for match in PROGRESS_THROUGHPUT_PATTERN.finditer(line))
        if any(pattern.search(line) for pattern in error_patterns):
            matched_error_indexes.add(index)
            categorized_indexes.add(index)
            record_event(
                make_event(
                    "PROCESS_FAILED",
                    experiment_id=experiment_id,
                    source=source,
                    severity="critical",
                    dedupe_key=f"process-failed:{hashlib.sha256(line.strip().encode()).hexdigest()[:20]}",
                    evidence_ref=byte_ref,
                    evidence=_evidence_window(previous_context, lines, index),
                    data={"message": _bounded_line(line)},
                    requires_sol=True,
                    now_epoch=now_epoch,
                )
            )
            continue
        if any(pattern.search(line) for pattern in scientific_patterns):
            categorized_indexes.add(index)
            record_event(
                make_event(
                    "SCIENTIFIC_REVIEW_REQUIRED",
                    experiment_id=experiment_id,
                    source=source,
                    severity="warning",
                    dedupe_key=f"scientific:{hashlib.sha256(line.strip().encode()).hexdigest()[:20]}",
                    evidence_ref=byte_ref,
                    evidence=_evidence_window(previous_context, lines, index),
                    data={"message": _bounded_line(line)},
                    requires_sol=True,
                    now_epoch=now_epoch,
                )
            )
            continue
        completion_patterns = (
            evaluation_complete_patterns if spec["phase"] == "EVALUATION" else training_complete_patterns
        )
        if any(pattern.search(line) for pattern in completion_patterns):
            categorized_indexes.add(index)
            event_type = "EVAL_COMPLETED" if spec["phase"] == "EVALUATION" else "TRAINING_COMPLETED"
            record_event(
                make_event(
                    event_type,
                    experiment_id=experiment_id,
                    source=source,
                    severity="info",
                    dedupe_key=f"{event_type.lower()}:{log_result['path']}",
                    evidence_ref=byte_ref,
                    evidence=_evidence_window(previous_context, lines, index),
                    data={"message": _bounded_line(line)},
                    requires_sol=True,
                    once=True,
                    now_epoch=now_epoch,
                )
            )
            continue
        if DEFAULT_CHECKPOINT_PATTERN.search(line):
            categorized_indexes.add(index)
            record_event(
                make_event(
                    "CHECKPOINT",
                    experiment_id=experiment_id,
                    source=source,
                    severity="info",
                    dedupe_key=f"checkpoint-line:{hashlib.sha256(line.strip().encode()).hexdigest()[:20]}",
                    evidence_ref=byte_ref,
                    evidence=_evidence_window(previous_context, lines, index),
                    data={"message": _bounded_line(line)},
                    now_epoch=now_epoch,
                )
            )
        if any(pattern.search(line) for pattern in known_warning_patterns):
            matched_known_indexes.add(index)
            categorized_indexes.add(index)
            record_event(
                make_event(
                    "KNOWN_WARNING",
                    experiment_id=experiment_id,
                    source=source,
                    severity="warning",
                    dedupe_key=f"known-warning:{hashlib.sha256(line.strip().encode()).hexdigest()[:20]}",
                    evidence_ref=byte_ref,
                    evidence=_evidence_window(previous_context, lines, index),
                    data={"message": _bounded_line(line)},
                    now_epoch=now_epoch,
                )
            )
        unknown_match = (
            any(pattern.search(line) for pattern in unknown_warning_patterns)
            if unknown_warning_patterns
            else DEFAULT_UNKNOWN_WARNING_PATTERN.search(line) is not None
        )
        if unknown_match and index not in matched_error_indexes and index not in matched_known_indexes:
            categorized_indexes.add(index)
            record_event(
                make_event(
                    "UNKNOWN_WARNING",
                    experiment_id=experiment_id,
                    source=source,
                    severity="warning",
                    dedupe_key=f"unknown-warning:{hashlib.sha256(line.strip().encode()).hexdigest()[:20]}",
                    evidence_ref=byte_ref,
                    evidence=_evidence_window(previous_context, lines, index),
                    data={"message": _bounded_line(line)},
                    requires_luna=True,
                    now_epoch=now_epoch,
                )
            )
        if index not in categorized_indexes and any(pattern.search(line) for pattern in milestone_patterns):
            record_event(
                make_event(
                    "MILESTONE",
                    experiment_id=experiment_id,
                    source=source,
                    severity="info",
                    dedupe_key=f"configured-milestone:{hashlib.sha256(line.strip().encode()).hexdigest()[:20]}",
                    evidence_ref=byte_ref,
                    evidence=_evidence_window(previous_context, lines, index),
                    data={"message": _bounded_line(line)},
                    requires_sol=True,
                    now_epoch=now_epoch,
                )
            )

    last_step = prior_step
    if observed_steps:
        observed_max = max(observed_steps)
        if prior_step is None or observed_max > prior_step:
            last_step = observed_max
            log_state["last_step_at_epoch"] = now_epoch
            record_event(
                make_event(
                    "PROGRESS",
                    experiment_id=experiment_id,
                    source=source,
                    severity="info",
                    dedupe_key=f"progress-step:{observed_max}",
                    evidence_ref=byte_ref,
                    data={"step": observed_max},
                    now_epoch=now_epoch,
                )
            )
            log_state["step_regression_polls"] = 0
        elif observed_max < prior_step - int(spec["thresholds"]["step_regression_tolerance"]):
            regression_polls = int(log_state.get("step_regression_polls", 0)) + 1
            log_state["step_regression_polls"] = regression_polls
            if regression_polls >= int(spec["thresholds"]["consecutive_violations"]):
                record_event(
                    make_event(
                        "INVARIANT_FAILED",
                        experiment_id=experiment_id,
                        source=source,
                        severity="warning",
                        dedupe_key=f"step-regression:{prior_step}:{observed_max}",
                        evidence_ref=byte_ref,
                        data={"previous_step": prior_step, "observed_step": observed_max},
                        requires_sol=True,
                        now_epoch=now_epoch,
                    )
                )
        else:
            log_state["step_regression_polls"] = 0
    log_state["last_step"] = last_step

    histories = {
        "loss": list(log_state.get("loss_history", [])),
        "gradient": list(log_state.get("gradient_history", [])),
        "throughput": list(log_state.get("throughput_history", [])),
    }
    metric_values = {
        "loss": new_losses,
        "gradient": new_gradients,
        "throughput": new_throughputs,
    }
    for metric, values in metric_values.items():
        nonfinite = [value for value in values if not math.isfinite(value)]
        if nonfinite:
            record_event(
                make_event(
                    "INVARIANT_FAILED",
                    experiment_id=experiment_id,
                    source=source,
                    severity="critical",
                    dedupe_key=f"nonfinite:{metric}",
                    evidence_ref=byte_ref,
                    data={"invariant": "finite", "metric": metric, "count": len(nonfinite)},
                    requires_sol=True,
                    once=True,
                    now_epoch=now_epoch,
                )
            )

    minimum = int(spec["thresholds"]["minimum_metric_samples"])
    required_consecutive = int(spec["thresholds"]["consecutive_violations"])
    window = int(spec["thresholds"]["metric_window"])
    for metric, values in metric_values.items():
        history = histories[metric]
        counter_key = f"{metric}_violation_samples"
        count = int(log_state.get(counter_key, 0))
        triggered_value: float | None = None
        triggered_count = 0
        for value in values:
            if not math.isfinite(value):
                continue
            if metric == "throughput":
                violated = _robust_lower_violation(
                    history,
                    value,
                    minimum=minimum,
                    ratio=float(spec["thresholds"]["throughput_ratio"]),
                )
            else:
                z_key = "loss_mad_z" if metric == "loss" else "gradient_mad_z"
                violated = _robust_upper_violation(
                    history,
                    value,
                    minimum=minimum,
                    z_limit=float(spec["thresholds"][z_key]),
                )
            count = count + 1 if violated else 0
            if count >= required_consecutive and triggered_value is None:
                triggered_value = value
                triggered_count = count
            history.append(value)
            history = history[-window:]
        # Empty polling cycles preserve the consecutive-sample counter. A fast
        # monitor must not erase a metric debounce merely because no new metric
        # was logged during that particular scheduler poll.
        log_state[counter_key] = count
        histories[metric] = history
        log_state[f"{metric}_history"] = history
        if triggered_value is not None:
            record_event(
                make_event(
                    "INVARIANT_FAILED",
                    experiment_id=experiment_id,
                    source=source,
                    severity="warning",
                    dedupe_key=f"robust-metric:{metric}",
                    evidence_ref=byte_ref,
                    data={
                        "invariant": "robust_metric_envelope",
                        "metric": metric,
                        "consecutive_violations": triggered_count,
                        "latest": triggered_value,
                    },
                    requires_sol=True,
                    now_epoch=now_epoch,
                )
            )
    if new_losses:
        log_state["last_loss"] = new_losses[-1] if math.isfinite(new_losses[-1]) else None
    if new_gradients:
        log_state["last_gradient_norm"] = (
            new_gradients[-1] if math.isfinite(new_gradients[-1]) else None
        )
    if new_throughputs:
        log_state["last_throughput"] = (
            new_throughputs[-1] if math.isfinite(new_throughputs[-1]) else None
        )
    log_state["context_tail"] = [*previous_context, *lines][-2:]
    log_result["progress"] = compact(
        {
            "last_step": last_step,
            "loss": _series_summary(new_losses),
            "gradient_norm": _series_summary(new_gradients),
            "throughput": _series_summary(new_throughputs),
        }
    )
    if event_overflow:
        events.append(
            make_event(
                "SCIENTIFIC_REVIEW_REQUIRED",
                experiment_id=experiment_id,
                source=source,
                severity="critical",
                dedupe_key=f"event-overflow:{byte_ref}",
                evidence_ref=byte_ref,
                data={
                    "condition": "bounded_event_parser_overflow",
                    "retained_events": MAX_PARSED_EVENTS_PER_LOG,
                    "omitted_events": event_overflow,
                },
                requires_sol=True,
                once=True,
                now_epoch=now_epoch,
            )
        )
    return events


def _resolve_monitor_path(raw_path: str, cwd: Path) -> Path:
    path = Path(raw_path).expanduser()
    return path if path.is_absolute() else cwd / path


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(1_048_576)
                if not chunk:
                    break
                digest.update(chunk)
    except OSError as exc:
        raise ClusterManagerError(f"cannot hash artifact {path}: {exc}") from exc
    return digest.hexdigest()


def artifact_events(
    *,
    state: dict[str, Any],
    spec: dict[str, Any],
    cwd: Path,
    terminal: bool,
    target_reached: bool,
    now_epoch: float,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for rule in spec["artifacts"]:
        path = _resolve_monitor_path(rule["path"], cwd)
        key = str(path)
        previous = dict(state["artifacts"].get(key, {}))
        exists = path.exists()
        current: dict[str, Any] = {"path": rule["path"], "exists": exists, "kind": rule["kind"]}
        changed = exists != bool(previous.get("exists"))
        if exists:
            try:
                stat = path.stat()
            except OSError as exc:
                events.append(
                    make_event(
                        "ARTIFACT_INVALID",
                        experiment_id=spec["experiment_id"],
                        source=f"artifact:{rule['path']}",
                        severity="critical",
                        dedupe_key=f"artifact-stat:{rule['path']}:{exc}",
                        evidence_ref=rule["path"],
                        data={"error": str(exc)[:500]},
                        requires_sol=True,
                        now_epoch=now_epoch,
                    )
                )
                state["artifacts"][key] = current
                continue
            current.update(
                {
                    "size": stat.st_size,
                    "mtime_ns": stat.st_mtime_ns,
                    "inode": stat.st_ino,
                    "path_type": "file" if path.is_file() else "directory" if path.is_dir() else "other",
                }
            )
            changed = changed or any(
                previous.get(field) != current[field] for field in ("size", "mtime_ns", "inode")
            )
            if rule["sha256"] and not path.is_file():
                events.append(
                    make_event(
                        "ARTIFACT_INVALID",
                        experiment_id=spec["experiment_id"],
                        source=f"artifact:{rule['path']}",
                        severity="critical",
                        dedupe_key=f"artifact-hash-not-file:{rule['path']}",
                        evidence_ref=rule["path"],
                        data={
                            "condition": "sha256_requires_regular_file",
                            "path_type": current["path_type"],
                        },
                        requires_sol=True,
                        once=True,
                        now_epoch=now_epoch,
                    )
                )
            elif rule["sha256"] and (changed or not previous.get("sha256")):
                current["sha256"] = _sha256_file(path)
            elif previous.get("sha256"):
                current["sha256"] = previous["sha256"]
            if rule["sha256"] and path.is_file() and current.get("sha256") != rule["sha256"]:
                events.append(
                    make_event(
                        "ARTIFACT_INVALID",
                        experiment_id=spec["experiment_id"],
                        source=f"artifact:{rule['path']}",
                        severity="critical",
                        dedupe_key=f"artifact-hash:{rule['path']}:{current.get('sha256', '')}",
                        evidence_ref=rule["path"],
                        data={"expected_sha256": rule["sha256"], "actual_sha256": current.get("sha256")},
                        requires_sol=True,
                        now_epoch=now_epoch,
                    )
                )
            elif changed:
                event_type = {
                    "checkpoint": "CHECKPOINT",
                    "evaluation_complete": "EVAL_COMPLETED",
                    "training_complete": "TRAINING_COMPLETED",
                }.get(rule["kind"], "MILESTONE")
                requires_sol = bool(rule["wake_on_create"])
                events.append(
                    make_event(
                        event_type,
                        experiment_id=spec["experiment_id"],
                        source=f"artifact:{rule['path']}",
                        severity="info",
                        dedupe_key=f"artifact-created:{rule['path']}:{current.get('inode')}:{current.get('mtime_ns')}",
                        evidence_ref=rule["path"],
                        data={field: current[field] for field in ("path", "size", "mtime_ns", "sha256") if field in current},
                        requires_sol=requires_sol,
                        once=True,
                        now_epoch=now_epoch,
                    )
                )
                if rule["kind"] == "checkpoint":
                    state["last_checkpoint"] = rule["path"]
        required_now = rule["required_when"] == "terminal" and terminal
        required_now = required_now or rule["required_when"] == "target_step" and target_reached
        if required_now and not exists:
            events.append(
                make_event(
                    "ARTIFACT_MISSING",
                    experiment_id=spec["experiment_id"],
                    source=f"artifact:{rule['path']}",
                    severity="critical",
                    dedupe_key=f"artifact-missing:{rule['path']}:{rule['required_when']}",
                    evidence_ref=rule["path"],
                    data={"required_when": rule["required_when"]},
                    requires_sol=True,
                    once=True,
                    now_epoch=now_epoch,
                )
            )
        state["artifacts"][key] = current
    return events


def process_events(
    *, state: dict[str, Any], spec: dict[str, Any], now_epoch: float
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for rule in spec["processes"]:
        pid = int(rule["pid"])
        alive = True
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            alive = False
        except PermissionError:
            alive = True
        except OSError:
            alive = False
        previous = state["processes"].get(str(pid), {})
        state["processes"][str(pid)] = {
            "pid": pid,
            "name": rule["name"],
            "alive": alive,
            "checked_at_epoch": now_epoch,
        }
        if rule["required"] and not alive and previous.get("alive", True):
            events.append(
                make_event(
                    "PROCESS_FAILED",
                    experiment_id=spec["experiment_id"],
                    source=f"pid:{pid}",
                    severity="critical",
                    dedupe_key=f"pid-dead:{pid}",
                    data={"pid": pid, "name": rule["name"]},
                    requires_sol=True,
                    once=True,
                    now_epoch=now_epoch,
                )
            )
    return events


def _monitor_state_view(state: dict[str, Any]) -> dict[str, Any]:
    return compact(
        {
            "experiment_id": state.get("experiment_id"),
            "phase": state.get("phase"),
            "scheduler_status": state.get("scheduler_status"),
            "last_step": state.get("last_step"),
            "target_step": state.get("target_step"),
            "last_checkpoint": state.get("last_checkpoint"),
            "last_loss": state.get("last_loss"),
            "warnings": list(state.get("warnings", []))[-8:],
            "pending_evaluations": state.get("pending_evaluations", []),
            "wake_conditions": state.get("wake_conditions", []),
        }
    )


def build_luna_packet(
    event: dict[str, Any], *, state: dict[str, Any], spec: dict[str, Any]
) -> dict[str, Any]:
    packet: dict[str, Any] = {
        "schema": 1,
        "task": "bounded_material_event_triage",
        "model": "gpt-5.6-luna",
        "current_state": _monitor_state_view(state),
        "event": event,
        "response_contract": {
            "classification": "ROUTINE|WARNING|FRONTIER_REQUIRED|UNKNOWN",
            "confidence": "number from 0.0 through 1.0",
            "summary": "short description",
            "recommended_action": "IGNORE|MONITOR|DETERMINISTIC_ACTION|ESCALATE",
            "reason": "brief reason",
        },
        "constraints": [
            "Classify only the supplied event and bounded evidence.",
            "Do not make a scientific decision.",
            "Return JSON matching response_contract.",
        ],
    }
    maximum = int(spec["luna_max_input_bytes"])
    encoded = json.dumps(packet, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if len(encoded) <= maximum:
        return packet
    reduced_event = dict(event)
    reduced_event["evidence"] = str(reduced_event.get("evidence", ""))[:1_000]
    data = dict(reduced_event.get("data", {}))
    if "message" in data:
        data["message"] = str(data["message"])[:500]
    reduced_event["data"] = data
    packet["event"] = compact(reduced_event)
    encoded = json.dumps(packet, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if len(encoded) > maximum:
        packet["current_state"] = {
            key: value
            for key, value in packet["current_state"].items()
            if key in {"experiment_id", "phase", "scheduler_status", "last_step", "target_step"}
        }
        encoded = json.dumps(packet, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if len(encoded) > maximum:
        raise ClusterManagerError("bounded Luna packet exceeds configured input budget")
    return packet


def build_sol_wake_packet(
    event: dict[str, Any], *, state: dict[str, Any], emitted: Sequence[dict[str, Any]], spec: dict[str, Any]
) -> dict[str, Any]:
    evidence_refs = list(
        dict.fromkeys(
            str(item.get("evidence_ref"))
            for item in emitted
            if item.get("evidence_ref")
        )
    )[:16]
    warnings = [
        {
            "event": item.get("event"),
            "severity": item.get("severity"),
            "summary": item.get("data", {}).get("message", ""),
        }
        for item in emitted
        if item.get("severity") in {"warning", "critical"}
    ][:8]
    return compact(
        {
            "schema": 1,
            "wake_reason": event["event"],
            "event_id": event["id"],
            "experiment_id": state["experiment_id"],
            "phase": state["phase"],
            "job_ids": spec["job_ids"],
            "scheduler_status": state.get("scheduler_status"),
            "step": state.get("last_step"),
            "target_step": state.get("target_step"),
            "checkpoint": state.get("last_checkpoint"),
            "last_loss": state.get("last_loss"),
            "warnings_since_last_review": warnings,
            "evidence_refs": evidence_refs,
            "event": event,
            "next_scientific_action": spec.get("next_scientific_action", ""),
        }
    )


def _select_routed_event(events: Sequence[dict[str, Any]]) -> dict[str, Any] | None:
    priority = {"SOL": 0, "LUNA": 1, "RECORD": 2}
    severity = {"critical": 0, "warning": 1, "info": 2}
    routed = [item for item in events if item.get("route") in {"SOL", "LUNA"}]
    if not routed:
        return None
    return min(
        routed,
        key=lambda item: (
            priority[str(item["route"])],
            severity[str(item["severity"])],
            str(item["timestamp"]),
            str(item["id"]),
        ),
    )


def build_event_report(
    job_ids: Sequence[str],
    *,
    user: str,
    cwd: Path,
    state: dict[str, Any],
    monitor_spec: dict[str, Any],
    explicit_logs: dict[str, Path],
    auto_log: bool,
    max_log_bytes: int,
    milestone_patterns: Sequence[re.Pattern[str]],
    error_patterns: Sequence[re.Pattern[str]],
    runner: Runner = default_runner,
    now_epoch: float | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    now_epoch = time.time() if now_epoch is None else now_epoch
    state = prepare_monitor_state(state, monitor_spec, now_epoch=now_epoch)
    pending_luna = state.get("pending_luna")
    if isinstance(pending_luna, dict):
        packet = pending_luna.get("packet")
        return (
            {
                "schema": SCHEMA_VERSION,
                "mode": "event_monitor",
                "checked_at": datetime.fromtimestamp(now_epoch, timezone.utc).replace(microsecond=0).isoformat(),
                "changed": False,
                "wake": True,
                "route": "LUNA",
                "wake_packet": packet,
                "events": [pending_luna.get("event", {})],
                "state": _monitor_state_view(state),
                "telemetry": dict(state["telemetry"]),
                "pending_luna_replayed": True,
            },
            state,
        )

    state["telemetry"]["monitor_poll_count"] += 1
    merged_logs = {
        job_id: _resolve_monitor_path(path, cwd)
        for job_id, path in monitor_spec["log_bindings"].items()
    }
    merged_logs.update(explicit_logs)
    scheduler_warnings: list[str] = []
    try:
        jobs, scheduler_warnings = query_jobs(job_ids, user=user, runner=runner)
    except ClusterManagerError as exc:
        if any(job_id not in merged_logs for job_id in job_ids):
            raise
        scheduler_warnings = [
            str(exc),
            "scheduler unavailable; monitoring explicitly bound logs only",
        ]
        jobs = [
            JobRecord(job_id=job_id, name="", state="UNKNOWN", source="log_only")
            for job_id in job_ids
        ]

    previous_jobs = state.get("jobs", {})
    next_jobs = dict(previous_jobs)
    output_jobs: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    any_terminal = False
    running = False
    last_steps: list[int] = []
    last_step_times: list[float] = []
    growth_times: list[float] = []
    last_losses: list[float] = []

    for job in jobs:
        previous = dict(previous_jobs.get(job.job_id, {}))
        previous_signature = previous.get("signature")
        signature = job.signature()
        first_seen = previous_signature is None
        state_changed = previous_signature is not None and previous_signature != signature
        job_restarted = bool(
            previous_signature
            and previous_signature.get("start")
            and signature.get("start")
            and previous_signature.get("start") != signature.get("start")
        )
        log_state = dict(previous.get("log", {}))
        log_result: dict[str, Any] | None = None
        log_path = find_log_path(
            job,
            explicit_logs=merged_logs,
            cwd=cwd,
            runner=runner,
            auto_log=auto_log,
        )
        if log_path is not None:
            log_result, log_state = scan_log(
                log_path,
                previous=log_state,
                cwd=cwd,
                max_bytes=max_log_bytes,
                milestone_patterns=milestone_patterns,
                error_patterns=error_patterns,
                include_lines=True,
                now_epoch=now_epoch,
            )
            lines = list(log_result.pop("_lines", []))
            log_reset = bool(log_result.get("rotated") or log_result.get("truncated"))
            if log_reset or job_restarted:
                reset_log_semantic_window(log_state, now_epoch=now_epoch)
            state["telemetry"]["bytes_read_incrementally"] += int(log_result.get("new_bytes", 0))
            candidates.extend(
                parse_log_events(
                    lines=lines,
                    log_result=log_result,
                    log_state=log_state,
                    spec=monitor_spec,
                    milestone_patterns=milestone_patterns,
                    error_patterns=error_patterns,
                    now_epoch=now_epoch,
                )
            )
            if log_result.get("rotated") or log_result.get("truncated"):
                reset_kind = "rotation" if log_result.get("rotated") else "truncation"
                candidates.append(
                    make_event(
                        "KNOWN_WARNING",
                        experiment_id=monitor_spec["experiment_id"],
                        source=f"log:{log_result['path']}",
                        severity="warning",
                        dedupe_key=f"log-{reset_kind}:{log_result['path']}:{log_result.get('inode')}",
                        evidence_ref=log_result["path"],
                        data={"condition": f"log_{reset_kind}"},
                        now_epoch=now_epoch,
                    )
                )
            if job_restarted:
                candidates.append(
                    make_event(
                        "KNOWN_WARNING",
                        experiment_id=monitor_spec["experiment_id"],
                        source=f"slurm:{job.job_id}",
                        severity="warning",
                        dedupe_key=f"job-restarted:{job.job_id}:{signature.get('start', '')}",
                        data={
                            "condition": "job_restarted",
                            "previous_start": previous_signature.get("start", ""),
                            "start": signature.get("start", ""),
                        },
                        now_epoch=now_epoch,
                    )
                )
        elif previous.get("log"):
            candidates.append(
                make_event(
                    "KNOWN_WARNING",
                    experiment_id=monitor_spec["experiment_id"],
                    source=f"scheduler:{job.job_id}",
                    severity="warning",
                    dedupe_key=f"log-not-found:{job.job_id}",
                    data={"condition": "registered_log_not_found"},
                    now_epoch=now_epoch,
                )
            )

        if state_changed:
            candidates.append(
                make_event(
                    "PROGRESS",
                    experiment_id=monitor_spec["experiment_id"],
                    source=f"slurm:{job.job_id}",
                    severity="info",
                    dedupe_key=f"scheduler-state:{job.job_id}:{signature['state']}:{signature.get('start', '')}",
                    data={
                        "previous_state": previous_signature.get("state", "UNKNOWN"),
                        "state": job.normalized_state,
                    },
                    now_epoch=now_epoch,
                )
            )
        if (
            monitor_spec["phase"] == "EVALUATION"
            and first_seen
            and job.normalized_state in {"RUNNING", "COMPLETING", "STAGE_OUT"}
        ):
            candidates.append(
                make_event(
                    "EVAL_STARTED",
                    experiment_id=monitor_spec["experiment_id"],
                    source=f"slurm:{job.job_id}",
                    severity="info",
                    dedupe_key=f"eval-started:{job.job_id}:{signature.get('start', '')}",
                    data={"job_id": job.job_id, "state": job.normalized_state},
                    once=True,
                    now_epoch=now_epoch,
                )
            )
        if job.failed:
            candidates.append(
                make_event(
                    "PROCESS_FAILED",
                    experiment_id=monitor_spec["experiment_id"],
                    source=f"slurm:{job.job_id}",
                    severity="critical",
                    dedupe_key=f"job-failed:{job.job_id}:{job.normalized_state}:{job.exit_code}",
                    data={"job_id": job.job_id, "state": job.normalized_state, "exit_code": job.exit_code},
                    requires_sol=True,
                    once=True,
                    now_epoch=now_epoch,
                )
            )
        elif job.terminal:
            event_type = "EVAL_COMPLETED" if monitor_spec["phase"] == "EVALUATION" else "TRAINING_COMPLETED"
            candidates.append(
                make_event(
                    event_type,
                    experiment_id=monitor_spec["experiment_id"],
                    source=f"slurm:{job.job_id}",
                    severity="info",
                    dedupe_key=f"job-complete:{job.job_id}:{job.end}:{job.exit_code}",
                    data={"job_id": job.job_id, "state": job.normalized_state, "exit_code": job.exit_code},
                    requires_sol=True,
                    once=True,
                    now_epoch=now_epoch,
                )
            )
        unknown_since_epoch: float | None = None
        if job.normalized_state == "UNKNOWN":
            unknown_since_epoch = float(previous.get("unknown_since_epoch", now_epoch))
            unknown_limit = float(monitor_spec["thresholds"]["scheduler_unknown_seconds"])
            if unknown_limit and now_epoch - unknown_since_epoch >= unknown_limit:
                candidates.append(
                    make_event(
                        "UNKNOWN_WARNING",
                        experiment_id=monitor_spec["experiment_id"],
                        source=f"slurm:{job.job_id}",
                        severity="warning",
                        dedupe_key=f"scheduler-unknown:{job.job_id}",
                        data={
                            "condition": "scheduler_state_unknown",
                            "seconds_unknown": int(now_epoch - unknown_since_epoch),
                            "job_id": job.job_id,
                        },
                        requires_luna=True,
                        now_epoch=now_epoch,
                    )
                )
        any_terminal = any_terminal or job.terminal
        running = running or job.normalized_state in {"RUNNING", "COMPLETING", "STAGE_OUT"}
        if isinstance(log_state.get("last_step"), int):
            last_steps.append(int(log_state["last_step"]))
        if isinstance(log_state.get("last_step_at_epoch"), (int, float)):
            last_step_times.append(float(log_state["last_step_at_epoch"]))
        if isinstance(log_state.get("last_growth_at_epoch"), (int, float)):
            growth_times.append(float(log_state["last_growth_at_epoch"]))
        if isinstance(log_state.get("last_loss"), (int, float)):
            last_losses.append(float(log_state["last_loss"]))

        item = asdict(job)
        item.update(
            {
                "state": job.normalized_state,
                "terminal": job.terminal,
                "anomaly": job.failed,
                "changed": state_changed,
            }
        )
        if state_changed:
            item["previous_state"] = previous_signature.get("state", "UNKNOWN")
        if log_result is not None:
            item["log"] = log_result
        output_jobs.append(compact(item))
        next_jobs[job.job_id] = compact(
            {
                "signature": signature,
                "log": log_state,
                "unknown_since_epoch": unknown_since_epoch,
            }
        )

    state["jobs"] = next_jobs
    states = [job.normalized_state for job in jobs]
    state["scheduler_status"] = states[0] if len(set(states)) == 1 and states else "MIXED" if states else "UNKNOWN"
    if last_steps:
        state["last_step"] = max(last_steps)
    if last_losses:
        state["last_loss"] = last_losses[-1]
    target_step = monitor_spec.get("target_step")
    target_reached = target_step is not None and isinstance(state.get("last_step"), int) and state["last_step"] >= target_step
    if target_reached:
        candidates.append(
            make_event(
                "MILESTONE",
                experiment_id=monitor_spec["experiment_id"],
                source="invariant:target_step",
                severity="info",
                dedupe_key=f"target-step:{target_step}",
                data={"step": state["last_step"], "target_step": target_step},
                requires_sol=True,
                once=True,
                now_epoch=now_epoch,
            )
        )

    if running:
        stall_seconds = float(monitor_spec["thresholds"]["stall_seconds"])
        log_stall_seconds = float(monitor_spec["thresholds"]["log_stall_seconds"])
        last_progress = max(last_step_times) if last_step_times else float(state.get("started_at_epoch", now_epoch))
        last_growth = max(growth_times) if growth_times else float(state.get("started_at_epoch", now_epoch))
        if stall_seconds and now_epoch - last_progress >= stall_seconds:
            candidates.append(
                make_event(
                    "STALL",
                    experiment_id=monitor_spec["experiment_id"],
                    source="invariant:step_progress",
                    severity="warning",
                    dedupe_key=f"step-stall:{state.get('last_step')}",
                    data={"seconds_without_progress": int(now_epoch - last_progress), "last_step": state.get("last_step")},
                    requires_sol=True,
                    now_epoch=now_epoch,
                )
            )
        if log_stall_seconds and now_epoch - last_growth >= log_stall_seconds:
            candidates.append(
                make_event(
                    "STALL",
                    experiment_id=monitor_spec["experiment_id"],
                    source="invariant:log_growth",
                    severity="warning",
                    dedupe_key=f"log-stall:{int(last_growth)}",
                    data={"seconds_without_log_growth": int(now_epoch - last_growth)},
                    requires_sol=True,
                    now_epoch=now_epoch,
                )
            )

    candidates.extend(
        artifact_events(
            state=state,
            spec=monitor_spec,
            cwd=cwd,
            terminal=any_terminal,
            target_reached=target_reached,
            now_epoch=now_epoch,
        )
    )
    candidates.extend(process_events(state=state, spec=monitor_spec, now_epoch=now_epoch))

    for warning in scheduler_warnings:
        known = "unable to contact" in warning.lower() or "monitoring explicitly bound logs" in warning.lower()
        candidates.append(
            make_event(
                "KNOWN_WARNING" if known else "UNKNOWN_WARNING",
                experiment_id=monitor_spec["experiment_id"],
                source="slurm:controller",
                severity="warning",
                dedupe_key=f"scheduler-warning:{hashlib.sha256(warning.encode()).hexdigest()[:20]}",
                data={"message": warning[:500]},
                requires_luna=not known,
                now_epoch=now_epoch,
            )
        )

    emitted = [
        event
        for event in candidates
        if accept_event(event, state=state, spec=monitor_spec, now_epoch=now_epoch)
    ]
    warning_summaries = [
        str(item.get("data", {}).get("message") or item.get("event"))[:500]
        for item in emitted
        if item.get("severity") in {"warning", "critical"}
    ]
    state["warnings"] = [*state.get("warnings", []), *warning_summaries][-16:]
    routed = _select_routed_event(emitted)
    route = str(routed["route"]) if routed else "RECORD"
    wake_packet: dict[str, Any] | None = None
    if routed and route == "LUNA":
        wake_packet = build_luna_packet(routed, state=state, spec=monitor_spec)
        queued_luna = [
            event
            for event in emitted
            if event.get("route") == "LUNA" and event.get("id") != routed.get("id")
        ]
        state["pending_luna"] = {
            "event": routed,
            "packet": wake_packet,
            "queue": queued_luna,
        }
        state["telemetry"]["luna_invocations"] += 1
    elif routed and route == "SOL":
        wake_packet = build_sol_wake_packet(routed, state=state, emitted=emitted, spec=monitor_spec)
        state["last_wake"] = wake_packet
        state["telemetry"]["sol_wakeups"] += 1

    if wake_packet is not None:
        handoff_bytes = len(json.dumps(wake_packet, sort_keys=True, separators=(",", ":")).encode("utf-8"))
        state["telemetry"]["handoff_bytes"] += handoff_bytes
    state["updated_at"] = datetime.fromtimestamp(now_epoch, timezone.utc).replace(microsecond=0).isoformat()
    state_view = _monitor_state_view(state)
    report = {
        "schema": SCHEMA_VERSION,
        "mode": "event_monitor",
        "checked_at": state["updated_at"],
        "changed": bool(emitted),
        "anomaly": any(item.get("severity") == "critical" for item in emitted),
        "wake": wake_packet is not None,
        "route": route,
        "jobs": output_jobs,
        "events": emitted[-16:],
        "state": state_view,
        "telemetry": dict(state["telemetry"]),
        "summary": {
            "total_jobs": len(output_jobs),
            "terminal_jobs": sum(bool(item.get("terminal")) for item in output_jobs),
            "events": len(emitted),
            "material_events": sum(item.get("route") in {"LUNA", "SOL"} for item in emitted),
        },
        "_event_records": emitted,
    }
    if wake_packet is not None:
        report["wake_packet"] = wake_packet
    if scheduler_warnings:
        report["warnings"] = scheduler_warnings
    return report, state


def persist_monitor_outputs(
    report: dict[str, Any], *, state: dict[str, Any], state_path: Path, spec: dict[str, Any], cwd: Path
) -> None:
    # Evidence and the wake packet are a write-ahead record. Advance the log
    # cursor only after they are durable; otherwise a crash between state and
    # evidence writes could permanently swallow a material event. A crash in
    # the opposite direction may append the same stable event ID twice, which
    # is recoverable and deterministic consumers can deduplicate.
    event_log = str(spec.get("event_log_path", ""))
    event_records = list(report.get("_event_records", []))
    if event_log and event_records:
        path = _resolve_monitor_path(event_log, cwd)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with path.open("a", encoding="utf-8") as handle:
                for event in event_records:
                    handle.write(json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
        except OSError as exc:
            raise ClusterManagerError(f"cannot append monitor event ledger {path}: {exc}") from exc
    wake_file = str(spec.get("wake_file", ""))
    if wake_file and report.get("wake_packet"):
        atomic_write_json(
            _resolve_monitor_path(wake_file, cwd),
            report["wake_packet"],
            maximum_bytes=MAX_MONITOR_STATE_BYTES,
        )
    save_state(state_path, state)


def public_monitor_report(report: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in report.items() if not key.startswith("_")}


def validate_luna_response(value: dict[str, Any]) -> dict[str, Any]:
    classification = str(value.get("classification", "")).strip().upper()
    action = str(value.get("recommended_action", "")).strip().upper()
    confidence = value.get("confidence")
    if classification not in LUNA_CLASSIFICATIONS:
        raise ClusterManagerError("Luna response classification is invalid")
    if action not in LUNA_ACTIONS:
        raise ClusterManagerError("Luna response recommended_action is invalid")
    if isinstance(confidence, bool) or not isinstance(confidence, (int, float)) or not math.isfinite(float(confidence)) or not 0 <= float(confidence) <= 1:
        raise ClusterManagerError("Luna response confidence must be between 0 and 1")
    summary = _require_monitor_text(value.get("summary"), "Luna response summary", maximum=1_000)
    reason = _require_monitor_text(value.get("reason"), "Luna response reason", maximum=2_000)
    return {
        "classification": classification,
        "confidence": float(confidence),
        "summary": summary,
        "recommended_action": action,
        "reason": reason,
    }


def resolve_luna_event(
    *,
    state: dict[str, Any],
    spec: dict[str, Any],
    response: dict[str, Any],
    event_id: str,
    input_tokens: int = 0,
    output_tokens: int = 0,
    now_epoch: float | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    now_epoch = time.time() if now_epoch is None else now_epoch
    state = prepare_monitor_state(state, spec, now_epoch=now_epoch)
    pending = state.get("pending_luna")
    if not isinstance(pending, dict) or pending.get("event", {}).get("id") != event_id:
        raise ClusterManagerError("no matching pending Luna event")
    validated = validate_luna_response(response)
    if input_tokens < 0 or output_tokens < 0:
        raise ClusterManagerError("Luna token counts must be non-negative")
    state["telemetry"]["luna_input_tokens"] += int(input_tokens)
    state["telemetry"]["luna_output_tokens"] += int(output_tokens)
    original = pending["event"]
    queued = list(pending.get("queue", []))
    minimum = float(spec["thresholds"]["luna_min_confidence"])
    frontier = (
        validated["classification"] in {"FRONTIER_REQUIRED", "UNKNOWN"}
        or validated["recommended_action"] in {"ESCALATE", "DETERMINISTIC_ACTION"}
        or validated["confidence"] < minimum
    )
    state["pending_luna"] = None
    resolution_timestamp = datetime.fromtimestamp(now_epoch, timezone.utc).replace(
        microsecond=0
    ).isoformat()
    state["last_luna_resolution"] = {
        "event_id": event_id,
        "resolved_at": resolution_timestamp,
        "classification": validated,
        "frontier_required": frontier,
    }
    result: dict[str, Any] = {
        "schema": SCHEMA_VERSION,
        "mode": "luna_resolution",
        "event_id": event_id,
        "classification": validated,
        "wake": frontier,
        "route": "SOL" if frontier else "RECORD",
    }
    if frontier:
        escalation = make_event(
            "SCIENTIFIC_REVIEW_REQUIRED",
            experiment_id=spec["experiment_id"],
            source="luna:triage",
            severity="warning",
            dedupe_key=f"luna-escalation:{event_id}",
            evidence_ref=str(original.get("evidence_ref", "")),
            data={"luna": validated, "original_event_id": event_id},
            requires_sol=True,
            once=True,
            now_epoch=now_epoch,
        )
        packet = build_sol_wake_packet(
            escalation,
            state=state,
            emitted=[original, *queued, escalation],
            spec=spec,
        )
        state["last_wake"] = packet
        state["telemetry"]["sol_wakeups"] += 1
        state["telemetry"]["events_emitted"] += 1
        state["telemetry"]["handoff_bytes"] += len(
            json.dumps(packet, sort_keys=True, separators=(",", ":")).encode("utf-8")
        )
        result["wake_packet"] = packet
    elif queued:
        next_event = queued[0]
        packet = build_luna_packet(next_event, state=state, spec=spec)
        state["pending_luna"] = {
            "event": next_event,
            "packet": packet,
            "queue": queued[1:],
        }
        state["telemetry"]["luna_invocations"] += 1
        state["telemetry"]["handoff_bytes"] += len(
            json.dumps(packet, sort_keys=True, separators=(",", ":")).encode("utf-8")
        )
        result["wake"] = True
        result["route"] = "LUNA"
        result["wake_packet"] = packet
    state["updated_at"] = resolution_timestamp
    result["telemetry"] = dict(state["telemetry"])
    return result, state


def compact(value: Any) -> Any:
    """Drop empty optional values while preserving false and zero."""
    if isinstance(value, dict):
        return {
            key: compact(item)
            for key, item in value.items()
            if item is not None and item != "" and item != [] and item != {}
        }
    if isinstance(value, list):
        return [compact(item) for item in value]
    return value


def filter_changes(report: dict[str, Any]) -> dict[str, Any]:
    filtered = dict(report)
    filtered["jobs"] = [job for job in report["jobs"] if job.get("changed") or job.get("anomaly")]
    return filtered


def render_report(report: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(compact(report), sort_keys=True, separators=(",", ":")))
        return
    if not report.get("jobs"):
        print("NO_CHANGES")
    for job in report.get("jobs", []):
        details = [job["job_id"], job["state"]]
        for key in ("elapsed", "location", "exit_code"):
            if job.get(key):
                details.append(f"{key}={job[key]}")
        if job.get("changed"):
            details.append("changed")
        if job.get("anomaly"):
            details.append("ANOMALY")
        print(" ".join(details))
        log = job.get("log", {})
        if log:
            print(f"  log={log.get('path')} size={log.get('size', 0)} new_bytes={log.get('new_bytes', 0)}")
            for line in log.get("milestones", []):
                print(f"  milestone: {line}")
            for line in log.get("errors", []):
                print(f"  error: {line}")
    summary = report.get("summary", {})
    print(
        "SUMMARY "
        f"total={summary.get('total', 0)} changed={summary.get('changed', 0)} "
        f"terminal={summary.get('terminal', 0)} anomalies={summary.get('anomalies', 0)}"
    )
    for warning in report.get("warnings", []):
        print(f"WARNING {warning}")


def query_resources(*, detailed: bool = False, runner: Runner = default_runner) -> dict[str, Any]:
    output = checked_output(
        ["sinfo", "-h", "-o", "%P|%a|%l|%D|%t|%G|%N"],
        runner,
    )
    rows: list[dict[str, str]] = []
    for line in output.splitlines():
        if not line.strip():
            continue
        fields = line.rstrip().split("|", 6)
        if len(fields) != 7:
            raise ClusterManagerError(f"unexpected sinfo row with {len(fields)} fields")
        partition, availability, limit, nodes, state, gres, node_list = fields
        rows.append(
            compact(
                {
                    "partition": partition.rstrip("*"),
                    "default": partition.endswith("*"),
                    "availability": availability,
                    "time_limit": limit,
                    "nodes": nodes,
                    "state": state,
                    "gres": gres,
                    "node_list": node_list,
                }
            )
        )
    if detailed:
        partitions: list[dict[str, Any]] = rows
    else:
        grouped: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        for row in rows:
            key = (
                row["partition"],
                row["availability"],
                row["time_limit"],
                row.get("gres", ""),
            )
            group = grouped.setdefault(
                key,
                {
                    "partition": row["partition"],
                    "availability": row["availability"],
                    "time_limit": row["time_limit"],
                    "gres": row.get("gres", ""),
                    "nodes": 0,
                    "states": {},
                },
            )
            node_count = int(row["nodes"])
            group["nodes"] += node_count
            group["states"][row["state"]] = group["states"].get(row["state"], 0) + node_count
        partitions = [compact(group) for group in grouped.values()]
    return {"schema": SCHEMA_VERSION, "checked_at": utc_now(), "partitions": partitions}


def render_resources(report: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(report, sort_keys=True, separators=(",", ":")))
        return
    for row in report["partitions"]:
        if "states" in row:
            states = ",".join(f"{state}:{nodes}" for state, nodes in sorted(row["states"].items()))
            print(
                f"{row['partition']} states={states} nodes={row['nodes']} "
                f"limit={row['time_limit']} gres={row.get('gres', '-')}"
            )
        else:
            print(
                f"{row['partition']} state={row['state']} nodes={row['nodes']} "
                f"limit={row['time_limit']} gres={row.get('gres', '-')} hosts={row.get('node_list', '-')}"
            )


def parse_gpu_nodes(output: str) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    for line in output.splitlines():
        fields: dict[str, str] = {}
        for token in line.split():
            if "=" in token:
                key, value = token.split("=", 1)
                fields[key] = value
        node_name = fields.get("NodeName")
        gres = fields.get("Gres", "")
        configured = fields.get("CfgTRES", "")
        allocated = fields.get("AllocTRES", "")
        total_match = re.search(r"(?:^|,)gres/gpu=([0-9]+)(?:,|$)", configured)
        if not node_name or not total_match:
            continue
        total = int(total_match.group(1))
        allocated_match = re.search(r"(?:^|,)gres/gpu=([0-9]+)(?:,|$)", allocated)
        used = int(allocated_match.group(1)) if allocated_match else 0
        type_match = re.search(r"(?:^|,)gpu:([^:,()]+):[0-9]+", gres)
        gpu_type = type_match.group(1) if type_match else "gpu"
        state = fields.get("State", "UNKNOWN").upper()
        partitions = [value for value in fields.get("Partitions", "").split(",") if value]
        nodes.append(
            compact(
                {
                    "node": node_name,
                    "gpu_type": gpu_type,
                    "state": state,
                    "total": total,
                    "allocated": used,
                    "free": max(0, total - used),
                    "partitions": partitions,
                }
            )
        )
    return sorted(nodes, key=lambda row: row["node"])


def parse_sinfo_node_states(output: str) -> dict[str, list[str]]:
    states: dict[str, set[str]] = {}
    for line in output.splitlines():
        if not line.strip():
            continue
        fields = line.rstrip().split("|", 1)
        if len(fields) != 2:
            raise ClusterManagerError(f"unexpected sinfo node row with {len(fields)} fields")
        node, state = fields
        states.setdefault(node, set()).add(state)
    return {node: sorted(values) for node, values in states.items()}


def node_is_schedulable(node: dict[str, Any]) -> bool:
    combined = " ".join([node.get("state", ""), *node.get("scheduler_states", [])]).upper()
    blocked_markers = ("DOWN", "DRAIN", "DRNG", "FAIL", "MAINT", "RESV", "RESERVED", "REBOOT", "POWER")
    return node.get("free", 0) > 0 and not any(marker in combined for marker in blocked_markers)


def query_gpus(*, available_only: bool = False, runner: Runner = default_runner) -> dict[str, Any]:
    nodes = parse_gpu_nodes(checked_output(["scontrol", "show", "nodes", "-o"], runner))
    warnings: list[str] = []
    try:
        scheduler_states = parse_sinfo_node_states(
            checked_output(["sinfo", "-N", "-h", "-o", "%N|%T"], runner)
        )
    except ClusterManagerError as exc:
        scheduler_states = {}
        warnings.append(str(exc))
    for node in nodes:
        node["scheduler_states"] = scheduler_states.get(node["node"], [])
        node["schedulable"] = node_is_schedulable(node)
    if available_only:
        nodes = [node for node in nodes if node["schedulable"]]
    by_type: dict[str, dict[str, int]] = {}
    for node in nodes:
        group = by_type.setdefault(
            node["gpu_type"],
            {"nodes": 0, "total": 0, "allocated": 0, "free": 0},
        )
        group["nodes"] += 1
        for key in ("total", "allocated", "free"):
            group[key] += int(node[key])
    report = {
        "schema": SCHEMA_VERSION,
        "checked_at": utc_now(),
        "summary": by_type,
        "nodes": nodes,
    }
    if warnings:
        report["warnings"] = warnings
    return report


def render_gpus(report: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(report, sort_keys=True, separators=(",", ":")))
        return
    for node in report["nodes"]:
        print(
            f"{node['node']} type={node['gpu_type']} state={node['state']} "
            f"gpus={node['allocated']}/{node['total']} free={node['free']} "
            f"schedulable={str(node['schedulable']).lower()} "
            f"partitions={','.join(node.get('partitions', []))}"
        )
    for warning in report.get("warnings", []):
        print(f"WARNING {warning}")


def common_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--json", action="store_true", help="emit one compact JSON object")
    parser.add_argument("--state-file", type=Path, help="override the persistent delta-state file")
    parser.add_argument("--no-state", action="store_true", help="do not read or write persistent state")
    parser.add_argument(
        "--monitor-spec",
        type=Path,
        help="enable event-driven monitoring from a validated JSON specification",
    )
    parser.add_argument("--log", action="append", default=[], metavar="JOB_ID=PATH", help="bind a job to a log")
    parser.add_argument("--no-auto-log", action="store_true", help="disable cwd and scontrol log discovery")
    parser.add_argument("--max-log-bytes", type=int, default=1_000_000, help="maximum new/tail bytes scanned per log")
    parser.add_argument("--milestone-regex", action="append", default=[], help="additional milestone expression")
    parser.add_argument(
        "--milestone-only-regex",
        action="append",
        default=[],
        help="replace default milestone expressions (repeatable; errors and terminal states still return)",
    )
    parser.add_argument("--error-regex", action="append", default=[], help="additional error expression")
    return parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Experiment-read-only Slurm, GPU, and log monitoring with atomic local cursor state."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  %(prog)s status 64001 64002 --logs --json
  %(prog)s watch 64001 --monitor-spec .research/monitors/RUN/spec.json
    --state-file .research/monitors/RUN/state.json --until wake --timeout 0 --json
  %(prog)s gpus --available-only --json
  %(prog)s resources --json
""",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    common = common_parser()

    status = subparsers.add_parser("status", parents=[common], help="show requested jobs, or all user jobs")
    status.add_argument("job_ids", nargs="*", help="Slurm job IDs; comma-separated IDs are accepted")
    status.add_argument("--user", default=getpass.getuser(), help="user for an unfiltered status listing")
    status.add_argument("--logs", action="store_true", help="scan bounded log deltas")
    status.add_argument("--changes-only", action="store_true", help="omit unchanged jobs")

    watch = subparsers.add_parser("watch", parents=[common], help="wait silently for a meaningful event")
    watch.add_argument("job_ids", nargs="*", help="Slurm job IDs; comma-separated IDs are accepted")
    watch.add_argument("--user", default=getpass.getuser())
    watch.add_argument("--interval", type=float, default=60.0, help="seconds between local checks")
    watch.add_argument("--timeout", type=float, default=0.0, help="maximum seconds; 0 means unlimited (required for event monitors)")
    watch.add_argument(
        "--until",
        choices=("wake", "event", "progress", "state", "milestone", "terminal", "anomaly"),
        default="wake",
        help="condition that makes the watcher return",
    )
    watch.add_argument("--emit-initial", action="store_true", help="print the baseline before waiting")

    luna = subparsers.add_parser(
        "resolve-luna",
        help="validate a bounded Luna triage response and deterministically continue or wake Sol",
    )
    luna.add_argument("--monitor-spec", type=Path, required=True)
    luna.add_argument("--state-file", type=Path, required=True)
    luna.add_argument("--event-id", required=True)
    response_group = luna.add_mutually_exclusive_group(required=True)
    response_group.add_argument("--response-json")
    response_group.add_argument("--response-file", type=Path)
    luna.add_argument("--input-tokens", type=int, default=0)
    luna.add_argument("--output-tokens", type=int, default=0)
    luna.add_argument("--json", action="store_true")

    resources = subparsers.add_parser("resources", help="show a compact sinfo partition summary")
    resources.add_argument("--json", action="store_true")
    resources.add_argument("--detailed", action="store_true", help="preserve one row per partition state")

    gpus = subparsers.add_parser("gpus", help="show GPU allocation by node from scontrol")
    gpus.add_argument("--json", action="store_true")
    gpus.add_argument("--available-only", action="store_true", help="show only nodes with free GPUs")
    return parser


def state_path_for(args: argparse.Namespace, cwd: Path) -> Path:
    return args.state_file.expanduser() if args.state_file else default_state_file(cwd)


def event_matches(report: dict[str, Any], condition: str) -> bool:
    if report.get("mode") == "event_monitor":
        if condition == "progress":
            return bool(report.get("changed"))
        # Event monitors return to an agent only for a material, routed wake.
        # Scheduler transitions and ordinary training progress remain internal.
        return bool(report.get("wake"))
    jobs = report.get("jobs", [])
    # An anomaly or terminal state always returns control, regardless of the
    # requested ordinary event, so a watcher cannot hide a finished job.
    if any(job.get("anomaly") for job in jobs):
        return True
    if any(job.get("terminal") for job in jobs):
        return True
    if condition == "anomaly":
        return False
    if condition == "terminal":
        return False
    if condition == "state":
        return any(job.get("previous_state") for job in jobs)
    if condition == "milestone":
        return any(job.get("log", {}).get("milestones") for job in jobs)
    return any(
        job.get("previous_state")
        or job.get("terminal")
        or job.get("anomaly")
        or job.get("log", {}).get("milestones")
        for job in jobs
    )


def collect_from_args(
    args: argparse.Namespace,
    *,
    job_ids: Sequence[str],
    cwd: Path,
    state: dict[str, Any],
    scan_logs: bool,
    monitor_spec: dict[str, Any] | None = None,
    runner: Runner = default_runner,
) -> tuple[dict[str, Any], dict[str, Any]]:
    explicit_logs = parse_log_bindings(args.log, job_ids, cwd)
    milestone_defaults = args.milestone_only_regex or (
        [] if monitor_spec is not None else DEFAULT_MILESTONE_PATTERNS
    )
    milestones = compile_patterns(milestone_defaults, args.milestone_regex)
    errors = compile_patterns(DEFAULT_ERROR_PATTERNS, args.error_regex)
    if args.max_log_bytes <= 0:
        raise ClusterManagerError("--max-log-bytes must be positive")
    if monitor_spec is not None:
        return build_event_report(
            job_ids,
            user=args.user,
            cwd=cwd,
            state=state,
            monitor_spec=monitor_spec,
            explicit_logs=explicit_logs,
            auto_log=not args.no_auto_log,
            max_log_bytes=args.max_log_bytes,
            milestone_patterns=milestones,
            error_patterns=errors,
            runner=runner,
        )
    return build_report(
        job_ids,
        user=args.user,
        cwd=cwd,
        state=state,
        scan_logs=scan_logs,
        explicit_logs=explicit_logs,
        auto_log=not args.no_auto_log,
        max_log_bytes=args.max_log_bytes,
        milestone_patterns=milestones,
        error_patterns=errors,
        runner=runner,
    )


def job_ids_and_spec(args: argparse.Namespace, cwd: Path) -> tuple[list[str], dict[str, Any] | None]:
    spec = load_monitor_spec(args.monitor_spec, cwd=cwd) if args.monitor_spec else None
    requested = validate_job_ids(args.job_ids)
    if spec is None:
        return requested, None
    configured = list(spec["job_ids"])
    if requested and configured and requested != configured:
        raise ClusterManagerError("command job IDs do not match the monitor specification")
    job_ids = requested or configured
    if not job_ids:
        raise ClusterManagerError("event monitor requires at least one Slurm job ID")
    return job_ids, spec


def _run_status_with_state(
    args: argparse.Namespace,
    *,
    cwd: Path,
    job_ids: Sequence[str],
    monitor_spec: dict[str, Any] | None,
    state_path: Path,
) -> int:
    state = {"schema": SCHEMA_VERSION, "jobs": {}} if args.no_state else load_state(state_path)
    report, next_state = collect_from_args(
        args,
        job_ids=job_ids,
        cwd=cwd,
        state=state,
        scan_logs=args.logs,
        monitor_spec=monitor_spec,
    )
    if not args.no_state:
        if monitor_spec is not None:
            persist_monitor_outputs(
                report,
                state=next_state,
                state_path=state_path,
                spec=monitor_spec,
                cwd=cwd,
            )
        else:
            save_state(state_path, next_state)
    shown = public_monitor_report(report) if monitor_spec is not None else report
    render_report(filter_changes(shown) if args.changes_only else shown, as_json=args.json)
    return 2 if report.get("anomaly", False) else 0


def run_status(args: argparse.Namespace) -> int:
    cwd = Path.cwd()
    job_ids, monitor_spec = job_ids_and_spec(args, cwd)
    if monitor_spec is not None and args.no_state:
        raise ClusterManagerError("event monitors require persistent state; --no-state is forbidden")
    state_path = state_path_for(args, cwd)
    lock = monitor_state_lock(state_path) if monitor_spec is not None else contextlib.nullcontext()
    with lock:
        return _run_status_with_state(
            args,
            cwd=cwd,
            job_ids=job_ids,
            monitor_spec=monitor_spec,
            state_path=state_path,
        )


def _run_watch_loop(
    args: argparse.Namespace,
    *,
    cwd: Path,
    job_ids: Sequence[str],
    monitor_spec: dict[str, Any] | None,
    state_path: Path,
) -> int:
    had_state = not args.no_state and state_path.exists()
    state = {"schema": SCHEMA_VERSION, "jobs": {}} if args.no_state else load_state(state_path)
    started = time.monotonic()

    report, state = collect_from_args(
        args,
        job_ids=job_ids,
        cwd=cwd,
        state=state,
        scan_logs=True,
        monitor_spec=monitor_spec,
    )
    if not args.no_state:
        if monitor_spec is not None:
            persist_monitor_outputs(
                report,
                state=state,
                state_path=state_path,
                spec=monitor_spec,
                cwd=cwd,
            )
        else:
            save_state(state_path, state)
    if args.emit_initial:
        render_report(
            public_monitor_report(report) if monitor_spec is not None else report,
            as_json=args.json,
        )
    if monitor_spec is not None and report.get("wake"):
        if not args.emit_initial:
            render_report(public_monitor_report(report), as_json=args.json)
        return 2 if report.get("anomaly") else 0
    if monitor_spec is None and (report["anomaly"] or any(job.get("terminal") for job in report["jobs"])):
        if not args.emit_initial:
            render_report(report, as_json=args.json)
        return 2 if report["anomaly"] else 0
    if had_state and event_matches(report, args.until):
        if not args.emit_initial:
            shown = public_monitor_report(report) if monitor_spec is not None else report
            render_report(filter_changes(shown), as_json=args.json)
        return 0

    while True:
        elapsed = time.monotonic() - started
        if args.timeout and elapsed >= args.timeout:
            timeout_report = {
                **report,
                "timed_out": True,
                "waited_seconds": round(elapsed, 1),
            }
            render_report(filter_changes(timeout_report), as_json=args.json)
            return 124
        sleep_for = args.interval
        if args.timeout:
            sleep_for = min(sleep_for, max(0.0, args.timeout - elapsed))
        backlog = any(
            int(job.get("log", {}).get("backlog_bytes", 0)) > 0
            for job in report.get("jobs", [])
        )
        if not backlog:
            time.sleep(sleep_for)
        report, state = collect_from_args(
            args,
            job_ids=job_ids,
            cwd=cwd,
            state=state,
            scan_logs=True,
            monitor_spec=monitor_spec,
        )
        if not args.no_state:
            if monitor_spec is not None:
                persist_monitor_outputs(
                    report,
                    state=state,
                    state_path=state_path,
                    spec=monitor_spec,
                    cwd=cwd,
                )
            else:
                save_state(state_path, state)
        if event_matches(report, args.until):
            shown = public_monitor_report(report) if monitor_spec is not None else report
            render_report(filter_changes(shown), as_json=args.json)
            return 2 if report.get("anomaly", False) else 0


def run_watch(args: argparse.Namespace) -> int:
    if args.interval <= 0:
        raise ClusterManagerError("--interval must be positive")
    if args.timeout < 0:
        raise ClusterManagerError("--timeout cannot be negative")

    cwd = Path.cwd()
    job_ids, monitor_spec = job_ids_and_spec(args, cwd)
    if monitor_spec is not None:
        if args.no_state:
            raise ClusterManagerError("event monitors require persistent state; --no-state is forbidden")
        if args.timeout:
            raise ClusterManagerError(
                "event monitors require --timeout 0; a finite timeout would return control merely because time passed"
            )
        if args.until != "wake":
            raise ClusterManagerError("event monitors require --until wake")
        if args.emit_initial:
            raise ClusterManagerError(
                "event monitors forbid --emit-initial because ordinary baseline state must stay silent"
            )
    state_path = state_path_for(args, cwd)
    lock = monitor_state_lock(state_path) if monitor_spec is not None else contextlib.nullcontext()
    with lock:
        return _run_watch_loop(
            args,
            cwd=cwd,
            job_ids=job_ids,
            monitor_spec=monitor_spec,
            state_path=state_path,
        )


def _read_luna_response(args: argparse.Namespace, cwd: Path) -> dict[str, Any]:
    if args.response_json is not None:
        raw = args.response_json
    else:
        path = args.response_file.expanduser()
        path = path if path.is_absolute() else cwd / path
        try:
            if path.stat().st_size > MAX_LUNA_PACKET_BYTES:
                raise ClusterManagerError("Luna response file exceeds the bounded response budget")
            raw = path.read_text(encoding="utf-8")
        except ClusterManagerError:
            raise
        except (OSError, UnicodeError) as exc:
            raise ClusterManagerError(f"cannot read Luna response file {path}: {exc}") from exc
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ClusterManagerError(f"Luna response is not valid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise ClusterManagerError("Luna response must contain one JSON object")
    return value


def run_resolve_luna(args: argparse.Namespace) -> int:
    cwd = Path.cwd()
    spec = load_monitor_spec(args.monitor_spec, cwd=cwd)
    state_path = args.state_file.expanduser()
    state_path = state_path if state_path.is_absolute() else cwd / state_path
    response = _read_luna_response(args, cwd)
    with monitor_state_lock(state_path):
        state = load_state(state_path)
        report, state = resolve_luna_event(
            state=state,
            spec=spec,
            response=response,
            event_id=args.event_id,
            input_tokens=args.input_tokens,
            output_tokens=args.output_tokens,
        )
        wake_file = str(spec.get("wake_file", ""))
        if wake_file:
            atomic_write_json(
                _resolve_monitor_path(wake_file, cwd),
                report.get("wake_packet", report),
                maximum_bytes=MAX_MONITOR_STATE_BYTES,
            )
        save_state(state_path, state)
    render_report(report, as_json=args.json)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "status":
            return run_status(args)
        if args.command == "watch":
            return run_watch(args)
        if args.command == "resolve-luna":
            return run_resolve_luna(args)
        if args.command == "resources":
            render_resources(query_resources(detailed=args.detailed), as_json=args.json)
            return 0
        if args.command == "gpus":
            render_gpus(query_gpus(available_only=args.available_only), as_json=args.json)
            return 0
        raise ClusterManagerError(f"unsupported command: {args.command}")
    except ClusterManagerError as exc:
        if getattr(args, "json", False):
            print(json.dumps({"error": str(exc)}, separators=(",", ":")))
        else:
            print(f"cluster-manager: {exc}", file=sys.stderr)
        return 3
    except KeyboardInterrupt:
        print("cluster-manager: interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
