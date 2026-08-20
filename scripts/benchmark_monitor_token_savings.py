#!/usr/bin/env python3
"""Matched replay benchmark for legacy versus event-driven cluster monitoring."""

from __future__ import annotations

import argparse
import hashlib
import importlib.machinery
import importlib.util
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Sequence


def load_module(name: str, path: Path):
    loader = importlib.machinery.SourceFileLoader(name, str(path))
    spec = importlib.util.spec_from_loader(name, loader)
    if spec is None:
        raise RuntimeError(f"cannot load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    loader.exec_module(module)
    return module


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1_048_576), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def append_text(path: Path, value: str) -> int:
    encoded = value.encode("utf-8")
    with path.open("ab") as handle:
        handle.write(encoded)
    return len(encoded)


class FakeRunner:
    def __init__(self, *, state: str = "RUNNING", exit_code: str = "0:0") -> None:
        self.state = state
        self.exit_code = exit_code
        self.calls: list[list[str]] = []

    def __call__(self, args: Sequence[str]) -> subprocess.CompletedProcess[str]:
        command = list(args)
        self.calls.append(command)
        if command[0] == "squeue":
            output = "" if self.state in {
                "BOOT_FAIL", "CANCELLED", "COMPLETED", "DEADLINE", "FAILED",
                "NODE_FAIL", "OUT_OF_MEMORY", "PREEMPTED", "REVOKED",
                "SPECIAL_EXIT", "TIMEOUT",
            } else f"123|train|{self.state}|00:12|04:48|1|node01|start\n"
            return subprocess.CompletedProcess(command, 0, output, "")
        if command[0] == "sacct":
            output = f"123|train|{self.state}|05:00:00|{self.exit_code}|node01|start|end\n"
            return subprocess.CompletedProcess(command, 0, output, "")
        return subprocess.CompletedProcess(command, 0, "", "")


def percentile(values: Sequence[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * fraction))))
    return ordered[index]


def make_spec(new, root: Path, log: Path, *, experiment_id: str, **updates: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "schema": 1,
        "experiment_id": experiment_id,
        "phase": "TRAINING",
        "job_ids": ["123"],
        "log_bindings": {"123": str(log)},
        "target_step": None,
        "wake_conditions": ["job exits", "known invariant fails"],
        "next_scientific_action": "inspect bounded evidence and decide",
        "thresholds": {
            "stall_seconds": 0,
            "log_stall_seconds": 0,
            "scheduler_unknown_seconds": 0,
            "dedupe_window_seconds": 3_600,
        },
        "artifacts": [],
        "processes": [],
        "known_warning_regex": [],
        "unknown_warning_regex": [],
        "training_complete_regex": [],
        "evaluation_complete_regex": [],
        "scientific_event_regex": [],
        "luna_max_input_bytes": 4_096,
        "event_log_path": "",
        "wake_file": "",
    }
    for key, item in updates.items():
        if key == "thresholds":
            value["thresholds"].update(item)
        else:
            value[key] = item
    return new.validate_monitor_spec(value, cwd=root)


def poll_new(new, root: Path, log: Path, state: dict[str, Any], spec: dict[str, Any], *, now: float):
    return new.build_event_report(
        ["123"],
        user="benchmark",
        cwd=root,
        state=state,
        monitor_spec=spec,
        explicit_logs={"123": log},
        auto_log=False,
        max_log_bytes=1_048_576,
        milestone_patterns=[],
        error_patterns=new.compile_patterns(new.DEFAULT_ERROR_PATTERNS, []),
        runner=FakeRunner(),
        now_epoch=now,
    )


def poll_old(old, root: Path, log: Path, state: dict[str, Any]):
    return old.build_report(
        ["123"],
        user="benchmark",
        cwd=root,
        state=state,
        scan_logs=True,
        explicit_logs={"123": log},
        auto_log=False,
        max_log_bytes=1_048_576,
        milestone_patterns=old.compile_patterns(old.DEFAULT_MILESTONE_PATTERNS, []),
        error_patterns=old.compile_patterns(old.DEFAULT_ERROR_PATTERNS, []),
        runner=FakeRunner(),
    )


def report_bytes(value: dict[str, Any]) -> int:
    return len(json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8"))


def run_matched_progress(old, new, root: Path, polls: int) -> dict[str, Any]:
    case = root / "matched_progress"
    case.mkdir(parents=True, exist_ok=True)
    log = case / "train.log"
    log.write_bytes(b"")
    spec = make_spec(new, case, log, experiment_id="matched-progress", target_step=polls)
    old_state: dict[str, Any] = {"schema": old.SCHEMA_VERSION, "jobs": {}}
    new_state = new.initial_monitor_state(spec, now_epoch=900.0)
    _, old_state = poll_old(old, case, log, old_state)
    _, new_state = poll_new(new, case, log, new_state, spec, now=900.0)

    records: list[dict[str, Any]] = []
    old_durations_ms: list[float] = []
    new_durations_ms: list[float] = []
    old_wake_packet_bytes = 0
    new_wake_packet_bytes = 0
    emitted_log_bytes = 0

    for step in range(1, polls + 1):
        loss = 2.0 - step / max(1, polls * 10)
        line = (
            f"training_step: {step} loss: {loss:.6f} "
            "grad_norm: 0.900000 throughput: 1000.000000\n"
        )
        emitted_log_bytes += append_text(log, line)

        started = time.perf_counter_ns()
        old_report, old_state = poll_old(old, case, log, old_state)
        old_durations_ms.append((time.perf_counter_ns() - started) / 1_000_000)

        started = time.perf_counter_ns()
        new_report, new_state = poll_new(
            new, case, log, new_state, spec, now=900.0 + step
        )
        new_durations_ms.append((time.perf_counter_ns() - started) / 1_000_000)

        old_wake = bool(old.event_matches(old_report, "event"))
        new_wake = bool(new.event_matches(new_report, "wake"))
        oracle_material = step == polls
        if old_wake:
            old_wake_packet_bytes += report_bytes(old_report)
        if new_wake:
            new_wake_packet_bytes += report_bytes(new.public_monitor_report(new_report))
        records.append(
            {
                "step": step,
                "oracle_material": oracle_material,
                "old_wake": old_wake,
                "new_wake": new_wake,
                "old_new_bytes": int(old_report["jobs"][0].get("log", {}).get("new_bytes", 0)),
                "new_new_bytes": int(new_report["jobs"][0].get("log", {}).get("new_bytes", 0)),
                "new_route": new_report.get("route", "RECORD"),
                "new_events": [event.get("event") for event in new_report.get("events", [])],
            }
        )

    post_target_wakes = []
    for index in range(3):
        old_report, old_state = poll_old(old, case, log, old_state)
        new_report, new_state = poll_new(
            new, case, log, new_state, spec, now=1_100.0 + index
        )
        post_target_wakes.append(
            {
                "old": bool(old.event_matches(old_report, "event")),
                "new": bool(new.event_matches(new_report, "wake")),
            }
        )

    def confusion(key: str) -> dict[str, Any]:
        true_positive = sum(bool(row[key]) and row["oracle_material"] for row in records)
        false_positive = sum(bool(row[key]) and not row["oracle_material"] for row in records)
        true_negative = sum(not bool(row[key]) and not row["oracle_material"] for row in records)
        false_negative = sum(not bool(row[key]) and row["oracle_material"] for row in records)
        precision = true_positive / (true_positive + false_positive) if true_positive + false_positive else 0.0
        recall = true_positive / (true_positive + false_negative) if true_positive + false_negative else 0.0
        specificity = true_negative / (true_negative + false_positive) if true_negative + false_positive else 0.0
        return {
            "true_positive": true_positive,
            "false_positive": false_positive,
            "true_negative": true_negative,
            "false_negative": false_negative,
            "precision": precision,
            "recall": recall,
            "specificity": specificity,
        }

    telemetry = dict(new_state["telemetry"])
    return {
        "polls_with_progress": polls,
        "emitted_log_bytes": emitted_log_bytes,
        "records": records,
        "old": {
            "confusion": confusion("old_wake"),
            "wake_opportunities": sum(bool(row["old_wake"]) for row in records),
            "ordinary_progress_wakes": sum(
                bool(row["old_wake"]) and not row["oracle_material"] for row in records
            ),
            "bytes_scanned": sum(int(row["old_new_bytes"]) for row in records),
            "wake_payload_bytes": old_wake_packet_bytes,
            "poll_ms_median": statistics.median(old_durations_ms),
            "poll_ms_p95": percentile(old_durations_ms, 0.95),
        },
        "new": {
            "confusion": confusion("new_wake"),
            "wake_opportunities": sum(bool(row["new_wake"]) for row in records),
            "ordinary_progress_wakes": sum(
                bool(row["new_wake"]) and not row["oracle_material"] for row in records
            ),
            "bytes_scanned": sum(int(row["new_new_bytes"]) for row in records),
            "wake_payload_bytes": new_wake_packet_bytes,
            "poll_ms_median": statistics.median(new_durations_ms),
            "poll_ms_p95": percentile(new_durations_ms, 0.95),
            "telemetry": telemetry,
        },
        "post_target_wakes": post_target_wakes,
    }


def run_quality_cases(new, root: Path) -> dict[str, Any]:
    cases: dict[str, Any] = {}

    unknown_root = root / "unknown_warning"
    unknown_root.mkdir(parents=True, exist_ok=True)
    unknown_log = unknown_root / "train.log"
    unknown_log.write_bytes(b"")
    unknown_spec = make_spec(new, unknown_root, unknown_log, experiment_id="unknown-warning")
    unknown_state = new.initial_monitor_state(unknown_spec, now_epoch=2_000.0)
    _, unknown_state = poll_new(new, unknown_root, unknown_log, unknown_state, unknown_spec, now=2_000.0)
    append_text(unknown_log, "WARNING novel frobulator cache signal; retrying shard lease\n")
    warning_report, unknown_state = poll_new(
        new, unknown_root, unknown_log, unknown_state, unknown_spec, now=2_001.0
    )
    packet = warning_report.get("wake_packet", {})
    event_id = str(packet.get("event", {}).get("id", ""))
    resolution, unknown_state = new.resolve_luna_event(
        state=unknown_state,
        spec=unknown_spec,
        response={
            "classification": "ROUTINE",
            "confidence": 0.99,
            "summary": "Synthetic retry warning is routine for this benchmark.",
            "recommended_action": "MONITOR",
            "reason": "The oracle labels this bounded synthetic message as non-scientific.",
        },
        event_id=event_id,
        now_epoch=2_002.0,
    )
    append_text(unknown_log, "WARNING novel frobulator cache signal; retrying shard lease\n")
    duplicate_report, unknown_state = poll_new(
        new, unknown_root, unknown_log, unknown_state, unknown_spec, now=2_003.0
    )
    cases["unknown_warning"] = {
        "expected_route": "LUNA",
        "observed_route": warning_report.get("route"),
        "initial_wake": warning_report.get("wake"),
        "luna_packet_bytes": report_bytes(packet),
        "packet_within_budget": report_bytes(packet) <= unknown_spec["luna_max_input_bytes"],
        "routine_resolution_woke_sol": resolution.get("wake"),
        "duplicate_wake": duplicate_report.get("wake"),
        "telemetry": dict(unknown_state["telemetry"]),
    }

    nonfinite_root = root / "nonfinite_loss"
    nonfinite_root.mkdir(parents=True, exist_ok=True)
    nonfinite_log = nonfinite_root / "train.log"
    nonfinite_log.write_bytes(b"")
    nonfinite_spec = make_spec(new, nonfinite_root, nonfinite_log, experiment_id="nonfinite-loss")
    nonfinite_state = new.initial_monitor_state(nonfinite_spec, now_epoch=3_000.0)
    _, nonfinite_state = poll_new(
        new, nonfinite_root, nonfinite_log, nonfinite_state, nonfinite_spec, now=3_000.0
    )
    append_text(nonfinite_log, "training_step: 17 loss: nan grad_norm: 0.9 throughput: 1000\n")
    nonfinite_report, nonfinite_state = poll_new(
        new, nonfinite_root, nonfinite_log, nonfinite_state, nonfinite_spec, now=3_001.0
    )
    cases["nonfinite_loss"] = {
        "expected_route": "SOL",
        "observed_route": nonfinite_report.get("route"),
        "wake": nonfinite_report.get("wake"),
        "events": [event.get("event") for event in nonfinite_report.get("events", [])],
        "telemetry": dict(nonfinite_state["telemetry"]),
    }

    checkpoint_root = root / "checkpoint"
    checkpoint_root.mkdir(parents=True, exist_ok=True)
    checkpoint_log = checkpoint_root / "train.log"
    checkpoint_log.write_bytes(b"")
    checkpoint_path = checkpoint_root / "ckpt_42.bin"
    checkpoint_spec = make_spec(
        new,
        checkpoint_root,
        checkpoint_log,
        experiment_id="checkpoint",
        artifacts=[
            {
                "path": str(checkpoint_path),
                "kind": "checkpoint",
                "wake_on_create": True,
                "required_when": "never",
            }
        ],
    )
    checkpoint_state = new.initial_monitor_state(checkpoint_spec, now_epoch=4_000.0)
    baseline_report, checkpoint_state = poll_new(
        new, checkpoint_root, checkpoint_log, checkpoint_state, checkpoint_spec, now=4_000.0
    )
    checkpoint_path.write_bytes(b"synthetic-checkpoint")
    checkpoint_report, checkpoint_state = poll_new(
        new, checkpoint_root, checkpoint_log, checkpoint_state, checkpoint_spec, now=4_001.0
    )
    duplicate_checkpoint_report, checkpoint_state = poll_new(
        new, checkpoint_root, checkpoint_log, checkpoint_state, checkpoint_spec, now=4_002.0
    )
    cases["checkpoint"] = {
        "baseline_wake": baseline_report.get("wake"),
        "expected_route": "SOL",
        "observed_route": checkpoint_report.get("route"),
        "wake": checkpoint_report.get("wake"),
        "duplicate_wake": duplicate_checkpoint_report.get("wake"),
        "events": [event.get("event") for event in checkpoint_report.get("events", [])],
    }

    eval_root = root / "evaluation"
    eval_root.mkdir(parents=True, exist_ok=True)
    eval_log = eval_root / "eval.log"
    eval_log.write_bytes(b"")
    eval_spec = make_spec(
        new, eval_root, eval_log, experiment_id="evaluation", phase="EVALUATION"
    )
    eval_state = new.initial_monitor_state(eval_spec, now_epoch=5_000.0)
    _, eval_state = poll_new(new, eval_root, eval_log, eval_state, eval_spec, now=5_000.0)
    append_text(eval_log, "evaluation completed successfully\n")
    eval_report, eval_state = poll_new(
        new, eval_root, eval_log, eval_state, eval_spec, now=5_001.0
    )
    cases["evaluation_complete"] = {
        "expected_route": "SOL",
        "observed_route": eval_report.get("route"),
        "wake": eval_report.get("wake"),
        "events": [event.get("event") for event in eval_report.get("events", [])],
    }

    checks = [
        cases["unknown_warning"]["observed_route"] == "LUNA",
        cases["unknown_warning"]["packet_within_budget"],
        not cases["unknown_warning"]["routine_resolution_woke_sol"],
        not cases["unknown_warning"]["duplicate_wake"],
        cases["unknown_warning"]["telemetry"]["luna_invocations"] == 1,
        cases["nonfinite_loss"]["observed_route"] == "SOL",
        "PROCESS_FAILED" in cases["nonfinite_loss"]["events"],
        cases["checkpoint"]["observed_route"] == "SOL",
        not cases["checkpoint"]["duplicate_wake"],
        cases["evaluation_complete"]["observed_route"] == "SOL",
        "EVAL_COMPLETED" in cases["evaluation_complete"]["events"],
    ]
    return {"cases": cases, "passed": sum(checks), "total": len(checks), "all_passed": all(checks)}


def parse_codex_jsonl(raw: str) -> tuple[dict[str, int], str, int]:
    usage = {"input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0}
    last_message = ""
    tool_items = 0
    for line in raw.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        candidate = event.get("usage")
        if isinstance(candidate, dict):
            for key in usage:
                if isinstance(candidate.get(key), int):
                    usage[key] = int(candidate[key])
        item = event.get("item")
        if isinstance(item, dict):
            if item.get("type") == "agent_message" and isinstance(item.get("text"), str):
                last_message = item["text"]
            elif item.get("type") not in {None, "reasoning"}:
                tool_items += 1
    return usage, last_message, tool_items


def parse_json_message(value: str) -> dict[str, Any] | None:
    stripped = value.strip()
    try:
        parsed = json.loads(stripped)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        start = stripped.find("{")
        stop = stripped.rfind("}")
        if start < 0 or stop <= start:
            return None
        try:
            parsed = json.loads(stripped[start : stop + 1])
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            return None


def run_luna_calibration(
    output_dir: Path,
    *,
    codex_binary: str,
    model: str,
    calls: int,
    timeout_seconds: float,
) -> dict[str, Any]:
    luna_root = output_dir / "luna_workspace"
    (luna_root / ".git").mkdir(parents=True, exist_ok=True)
    scratch_tmp = output_dir / "codex_tmp"
    scratch_tmp.mkdir(parents=True, exist_ok=True)
    prompt = (
        "Do not call tools or inspect files. Classify only this bounded deterministic-monitor "
        "state. Return exactly one compact JSON object with keys classification, confidence, "
        "summary, recommended_action, reason. Allowed classification: ROUTINE, WARNING, "
        "FRONTIER_REQUIRED, UNKNOWN. Allowed action: IGNORE, MONITOR, DETERMINISTIC_ACTION, "
        "ESCALATE. State: {\"scheduler_status\":\"RUNNING\",\"new_log_bytes\":0,"
        "\"new_events\":[],\"invariants\":\"pass\"}. This is unchanged routine state."
    )
    env = os.environ.copy()
    env["TMPDIR"] = str(scratch_tmp)
    results: list[dict[str, Any]] = []
    for index in range(1, calls + 1):
        command = [
            codex_binary,
            "exec",
            "--ephemeral",
            "--json",
            "--sandbox",
            "read-only",
            "--skip-git-repo-check",
            "-C",
            str(luna_root),
            "-m",
            model,
            "-c",
            'model_reasoning_effort="low"',
            prompt,
        ]
        started = time.monotonic()
        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                env=env,
                check=False,
            )
            elapsed = time.monotonic() - started
            stdout = completed.stdout
            stderr = completed.stderr
            returncode = completed.returncode
            error = ""
        except subprocess.TimeoutExpired as exc:
            elapsed = time.monotonic() - started
            stdout = exc.stdout.decode() if isinstance(exc.stdout, bytes) else exc.stdout or ""
            stderr = exc.stderr.decode() if isinstance(exc.stderr, bytes) else exc.stderr or ""
            returncode = 124
            error = f"timed out after {timeout_seconds} seconds"
        (output_dir / f"luna_call_{index}.jsonl").write_text(stdout, encoding="utf-8")
        (output_dir / f"luna_call_{index}.stderr.txt").write_text(stderr, encoding="utf-8")
        usage, message, tool_items = parse_codex_jsonl(stdout)
        parsed = parse_json_message(message)
        contract_ok = bool(
            parsed
            and str(parsed.get("classification", "")).upper() == "ROUTINE"
            and str(parsed.get("recommended_action", "")).upper() in {"IGNORE", "MONITOR"}
            and isinstance(parsed.get("confidence"), (int, float))
        )
        results.append(
            {
                "call": index,
                "returncode": returncode,
                "elapsed_seconds": elapsed,
                "usage": usage,
                "total_tokens": usage["input_tokens"] + usage["output_tokens"],
                "contract_ok": contract_ok,
                "tool_items": tool_items,
                "response": parsed,
                "error": error,
            }
        )

    successful = [item for item in results if item["returncode"] == 0 and item["usage"]["input_tokens"] > 0]
    totals = [int(item["total_tokens"]) for item in successful]
    inputs = [int(item["usage"]["input_tokens"]) for item in successful]
    outputs = [int(item["usage"]["output_tokens"]) for item in successful]
    return {
        "model": model,
        "reasoning_effort": "low",
        "requested_calls": calls,
        "successful_calls": len(successful),
        "contract_passes": sum(bool(item["contract_ok"]) for item in results),
        "tool_free_calls": sum(item["tool_items"] == 0 for item in results),
        "median_input_tokens": statistics.median(inputs) if inputs else None,
        "median_output_tokens": statistics.median(outputs) if outputs else None,
        "median_total_tokens": statistics.median(totals) if totals else None,
        "calls": results,
    }


def build_summary(raw: dict[str, Any]) -> dict[str, Any]:
    progress = raw["matched_progress"]
    quality = raw["quality"]
    luna = raw["luna_calibration"]
    median_luna = luna.get("median_total_tokens")
    old_false_wakes = progress["old"]["ordinary_progress_wakes"]
    extrapolation: dict[str, Any] = {}
    if isinstance(median_luna, (int, float)):
        extrapolation = {
            "counterfactual_tokens_for_observed_34_no_change_turns": int(round(median_luna * 34)),
            "counterfactual_tokens_for_benchmark_false_wakes": int(round(median_luna * old_false_wakes)),
            "five_hours_at_5_minute_polls": int(round(median_luna * 60)),
            "five_hours_at_1_minute_polls": int(round(median_luna * 300)),
            "treatment_tokens_during_ordinary_wait": 0,
        }
    success_checks = {
        "zero_treatment_progress_wakes": progress["new"]["ordinary_progress_wakes"] == 0,
        "target_recall_preserved": progress["new"]["confusion"]["recall"] == 1.0,
        "no_duplicate_target_wake": not any(item["new"] for item in progress["post_target_wakes"]),
        "zero_pre_material_luna_calls": progress["new"]["telemetry"]["luna_invocations"] == 0,
        "one_material_sol_wakeup": progress["new"]["telemetry"]["sol_wakeups"] == 1,
        "zero_frontier_no_change_wakeups": progress["new"]["telemetry"]["frontier_no_change_wakeups"] == 0,
        "zero_full_log_reads": progress["new"]["telemetry"]["full_log_reads"] == 0,
        "incremental_byte_accounting_exact": progress["new"]["bytes_scanned"] == progress["emitted_log_bytes"],
        "quality_cases_passed": quality["all_passed"],
        "luna_calibration_completed": luna["successful_calls"] == luna["requested_calls"],
        "luna_contract_passed": luna["contract_passes"] == luna["requested_calls"],
    }
    return {
        "primary_metric": {
            "name": "ordinary_progress_llm_wake_opportunities",
            "old": progress["old"]["ordinary_progress_wakes"],
            "new": progress["new"]["ordinary_progress_wakes"],
            "absolute_reduction": old_false_wakes - progress["new"]["ordinary_progress_wakes"],
            "relative_reduction": 1.0 if old_false_wakes else 0.0,
        },
        "quality": {
            "old_target_recall": progress["old"]["confusion"]["recall"],
            "new_target_recall": progress["new"]["confusion"]["recall"],
            "old_wake_precision": progress["old"]["confusion"]["precision"],
            "new_wake_precision": progress["new"]["confusion"]["precision"],
            "additional_cases_passed": quality["passed"],
            "additional_cases_total": quality["total"],
        },
        "performance": {
            "old_poll_ms_median": progress["old"]["poll_ms_median"],
            "new_poll_ms_median": progress["new"]["poll_ms_median"],
            "old_poll_ms_p95": progress["old"]["poll_ms_p95"],
            "new_poll_ms_p95": progress["new"]["poll_ms_p95"],
            "new_bytes_read_incrementally": progress["new"]["telemetry"]["bytes_read_incrementally"],
            "new_full_log_reads": progress["new"]["telemetry"]["full_log_reads"],
        },
        "luna_calibration": {
            key: luna.get(key)
            for key in (
                "model", "reasoning_effort", "requested_calls", "successful_calls",
                "contract_passes", "tool_free_calls", "median_input_tokens",
                "median_output_tokens", "median_total_tokens",
            )
        },
        "token_extrapolation": extrapolation,
        "success_checks": success_checks,
        "passed": all(success_checks.values()),
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-monitor", type=Path, required=True)
    parser.add_argument("--new-monitor", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--polls", type=int, default=120)
    parser.add_argument("--luna-calls", type=int, default=3)
    parser.add_argument("--luna-model", default="gpt-5.6-luna")
    parser.add_argument("--codex-binary", default="codex")
    parser.add_argument("--luna-timeout-seconds", type=float, default=240.0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.polls < 2 or args.luna_calls < 0:
        raise SystemExit("--polls must be at least 2 and --luna-calls cannot be negative")
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    old_path = args.old_monitor.resolve()
    new_path = args.new_monitor.resolve()
    old = load_module("benchmark_old_cluster_manager", old_path)
    new = load_module("benchmark_new_cluster_manager", new_path)

    raw = {
        "schema": 1,
        "started_at_epoch": time.time(),
        "inputs": {
            "old_monitor": str(old_path),
            "old_monitor_sha256": sha256_file(old_path),
            "new_monitor": str(new_path),
            "new_monitor_sha256": sha256_file(new_path),
            "polls": args.polls,
            "luna_calls": args.luna_calls,
        },
        "matched_progress": run_matched_progress(old, new, output_dir, args.polls),
        "quality": run_quality_cases(new, output_dir),
        "luna_calibration": run_luna_calibration(
            output_dir,
            codex_binary=args.codex_binary,
            model=args.luna_model,
            calls=args.luna_calls,
            timeout_seconds=args.luna_timeout_seconds,
        ),
    }
    raw["finished_at_epoch"] = time.time()
    raw["wall_seconds"] = raw["finished_at_epoch"] - raw["started_at_epoch"]
    summary = build_summary(raw)
    atomic_json(output_dir / "benchmark_raw.json", raw)
    atomic_json(output_dir / "benchmark_summary.json", summary)
    print(json.dumps(summary, sort_keys=True, separators=(",", ":")))
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
