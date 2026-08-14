#!/usr/bin/env python3
"""Compact, read-only Slurm status and log monitoring.

The CLI replaces recurring hand-written combinations of ``squeue``, ``sacct``,
``scontrol``, ``tail``, and ``rg`` with stable commands:

    scripts/cluster_manager.py status 64001 64002 --logs --json
    scripts/cluster_manager.py watch 64001 64002 --until event --json
    scripts/cluster_manager.py gpus --available-only --json
    scripts/cluster_manager.py resources --json

State is kept outside /tmp so separate invocations can report only meaningful
changes.  The tool never submits, cancels, requeues, or edits a Slurm job.
"""

from __future__ import annotations

import argparse
import getpass
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence


SCHEMA_VERSION = 1
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
    r"\btraining_step\s*[:=]\s*[0-9]+",
    r"\bglobal_step\s*[:=]\s*[0-9]+",
    r"\b(?:train(?:ing)?[_ /-]?)?loss\s*[:=]\s*[-+0-9.e]+",
    r"\b(?:grad(?:ient)?[_ /-]?norm|throughput|tokens/s|samples/s|it/s)\s*[:=]\s*[-+0-9.e]+",
    r"\bbatch_complete\b",
    r"\boptimizer(?:\s+|_)(?:step|update).*(?:complete|finished|done)\b",
    r"\b(?:saving|saved)\b.*\b(?:model|checkpoint)\b",
    r"\baudit\b.*\b(?:pass(?:ed)?|complete(?:d)?)\b",
    r"\bcomplete(?:d)? successfully\b",
    r"\[(?:done|ok)\]",
)


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
    if payload.get("schema") != SCHEMA_VERSION or not isinstance(payload.get("jobs"), dict):
        return {"schema": SCHEMA_VERSION, "jobs": {}}
    return payload


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(json.dumps(state, sort_keys=True, separators=(",", ":")) + "\n")
        os.replace(temporary, path)
    except OSError as exc:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise ClusterManagerError(f"cannot write state file {path}: {exc}") from exc


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


def scan_log(
    path: Path,
    *,
    previous: dict[str, Any],
    cwd: Path,
    max_bytes: int,
    milestone_patterns: Sequence[re.Pattern[str]],
    error_patterns: Sequence[re.Pattern[str]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    result: dict[str, Any] = {"path": display_path(path, cwd)}
    if not path.exists():
        result["missing"] = True
        return result, {"path": str(path), "size": 0, "mtime_ns": 0}
    if not path.is_file():
        result["not_file"] = True
        return result, {"path": str(path), "size": 0, "mtime_ns": 0}

    stat = path.stat()
    old_size = int(previous.get("size", 0)) if previous.get("path") == str(path) else 0
    if stat.st_size < old_size:
        old_size = 0
        result["rotated"] = True
    start = old_size
    if start == 0:
        start = max(0, stat.st_size - max_bytes)
    if stat.st_size - start > max_bytes:
        start = stat.st_size - max_bytes
        result["truncated"] = True

    with path.open("rb") as handle:
        handle.seek(start)
        raw = handle.read(max_bytes)
    text = raw.decode("utf-8", errors="replace")
    lines = text.splitlines()
    milestone_candidates = matching_lines(lines, milestone_patterns)
    error_candidates = matching_lines(lines, error_patterns)
    seen_milestones = list(previous.get("seen_milestones", []))
    seen_errors = list(previous.get("seen_errors", []))
    milestone_seen_set = set(seen_milestones)
    error_seen_set = set(seen_errors)
    milestones = [line for line in milestone_candidates if line not in milestone_seen_set]
    errors = [line for line in error_candidates if line not in error_seen_set]

    result["size"] = stat.st_size
    result["new_bytes"] = max(0, stat.st_size - old_size)
    if milestones:
        result["milestones"] = milestones
    if errors:
        result["errors"] = errors
    next_state = {
        "path": str(path),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "seen_milestones": [*seen_milestones, *milestone_candidates][-64:],
        "seen_errors": [*seen_errors, *error_candidates][-64:],
    }
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
    jobs, warnings = query_jobs(job_ids, user=user, runner=runner)
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
    parser.add_argument("--log", action="append", default=[], metavar="JOB_ID=PATH", help="bind a job to a log")
    parser.add_argument("--no-auto-log", action="store_true", help="disable cwd and scontrol log discovery")
    parser.add_argument("--max-log-bytes", type=int, default=1_000_000, help="maximum new/tail bytes scanned per log")
    parser.add_argument("--milestone-regex", action="append", default=[], help="additional milestone expression")
    parser.add_argument("--error-regex", action="append", default=[], help="additional error expression")
    return parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read-only, compact Slurm status, GPU, and log monitoring.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  %(prog)s status 64001 64002 --logs --json
  %(prog)s watch 64001 --until event --timeout 3600 --json
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
    watch.add_argument("job_ids", nargs="+", help="Slurm job IDs; comma-separated IDs are accepted")
    watch.add_argument("--user", default=getpass.getuser())
    watch.add_argument("--interval", type=float, default=60.0, help="seconds between local checks")
    watch.add_argument("--timeout", type=float, default=3600.0, help="maximum seconds; 0 means unlimited")
    watch.add_argument(
        "--until",
        choices=("event", "state", "milestone", "terminal", "anomaly"),
        default="event",
        help="condition that makes the watcher return",
    )
    watch.add_argument("--emit-initial", action="store_true", help="print the baseline before waiting")

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
    runner: Runner = default_runner,
) -> tuple[dict[str, Any], dict[str, Any]]:
    explicit_logs = parse_log_bindings(args.log, job_ids, cwd)
    milestones = compile_patterns(DEFAULT_MILESTONE_PATTERNS, args.milestone_regex)
    errors = compile_patterns(DEFAULT_ERROR_PATTERNS, args.error_regex)
    if args.max_log_bytes <= 0:
        raise ClusterManagerError("--max-log-bytes must be positive")
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


def run_status(args: argparse.Namespace) -> int:
    cwd = Path.cwd()
    job_ids = validate_job_ids(args.job_ids)
    state_path = state_path_for(args, cwd)
    state = {"schema": SCHEMA_VERSION, "jobs": {}} if args.no_state else load_state(state_path)
    report, next_state = collect_from_args(
        args,
        job_ids=job_ids,
        cwd=cwd,
        state=state,
        scan_logs=args.logs,
    )
    if not args.no_state:
        save_state(state_path, next_state)
    render_report(filter_changes(report) if args.changes_only else report, as_json=args.json)
    return 2 if report["anomaly"] else 0


def run_watch(args: argparse.Namespace) -> int:
    if args.interval <= 0:
        raise ClusterManagerError("--interval must be positive")
    if args.timeout < 0:
        raise ClusterManagerError("--timeout cannot be negative")

    cwd = Path.cwd()
    job_ids = validate_job_ids(args.job_ids)
    state_path = state_path_for(args, cwd)
    had_state = not args.no_state and state_path.exists()
    state = {"schema": SCHEMA_VERSION, "jobs": {}} if args.no_state else load_state(state_path)
    started = time.monotonic()

    report, state = collect_from_args(
        args,
        job_ids=job_ids,
        cwd=cwd,
        state=state,
        scan_logs=True,
    )
    if not args.no_state:
        save_state(state_path, state)
    if args.emit_initial:
        render_report(report, as_json=args.json)
    if report["anomaly"] or any(job.get("terminal") for job in report["jobs"]):
        if not args.emit_initial:
            render_report(report, as_json=args.json)
        return 2 if report["anomaly"] else 0
    if had_state and event_matches(report, args.until):
        if not args.emit_initial:
            render_report(filter_changes(report), as_json=args.json)
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
        time.sleep(sleep_for)
        report, state = collect_from_args(
            args,
            job_ids=job_ids,
            cwd=cwd,
            state=state,
            scan_logs=True,
        )
        if not args.no_state:
            save_state(state_path, state)
        if event_matches(report, args.until):
            render_report(filter_changes(report), as_json=args.json)
            return 2 if report["anomaly"] else 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "status":
            return run_status(args)
        if args.command == "watch":
            return run_watch(args)
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
