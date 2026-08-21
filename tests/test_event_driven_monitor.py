from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import cluster_manager as cm
from scripts import research_manager as rm


class FakeRunner:
    def __init__(self, *, state: str = "RUNNING", exit_code: str = "0:0") -> None:
        self.state = state
        self.exit_code = exit_code
        self.calls: list[list[str]] = []

    def __call__(self, args):
        command = list(args)
        self.calls.append(command)
        if command[0] == "squeue":
            if self.state in cm.TERMINAL_STATES:
                output = ""
            else:
                output = f"123|train|{self.state}|00:12|04:48|1|node01|start\n"
            return subprocess.CompletedProcess(command, 0, output, "")
        if command[0] == "sacct":
            output = f"123|train|{self.state}|05:00:00|{self.exit_code}|node01|start|end\n"
            return subprocess.CompletedProcess(command, 0, output, "")
        return subprocess.CompletedProcess(command, 0, "", "")


class EventDrivenMonitorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.log = self.root / "train.log"
        self.runner = FakeRunner()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def spec(self, **updates):
        value = {
            "schema": 1,
            "experiment_id": "exp-001",
            "phase": "TRAINING",
            "job_ids": ["123"],
            "log_bindings": {"123": str(self.log)},
            "target_step": None,
            "wake_conditions": ["job exits", "known invariant fails"],
            "next_scientific_action": "reduce and audit the completed run",
            "thresholds": {
                "stall_seconds": 0,
                "log_stall_seconds": 0,
                "dedupe_window_seconds": 3600,
            },
            "artifacts": [],
            "processes": [],
            "known_warning_regex": [],
            "unknown_warning_regex": [],
            "training_complete_regex": [],
            "evaluation_complete_regex": [],
            "scientific_event_regex": [],
            "luna_max_input_bytes": 4096,
            "event_log_path": "",
            "wake_file": "",
        }
        for key, item in updates.items():
            if key == "thresholds":
                value["thresholds"].update(item)
            else:
                value[key] = item
        return cm.validate_monitor_spec(value, cwd=self.root)

    def poll(self, state, spec, *, now=1000.0, runner=None, max_bytes=10_000):
        return cm.build_event_report(
            ["123"],
            user="alice",
            cwd=self.root,
            state=state,
            monitor_spec=spec,
            explicit_logs={"123": self.log},
            auto_log=False,
            max_log_bytes=max_bytes,
            milestone_patterns=[],
            error_patterns=cm.compile_patterns(cm.DEFAULT_ERROR_PATTERNS, []),
            runner=runner or self.runner,
            now_epoch=now,
        )

    @staticmethod
    def routine_response():
        return {
            "classification": "ROUTINE",
            "confidence": 0.99,
            "summary": "Benign library warning.",
            "recommended_action": "MONITOR",
            "reason": "No failure or scientific signal is present.",
        }

    @staticmethod
    def frontier_response():
        return {
            "classification": "FRONTIER_REQUIRED",
            "confidence": 0.94,
            "summary": "The warning conflicts with a registered invariant.",
            "recommended_action": "ESCALATE",
            "reason": "Scientific context is required.",
        }

    def test_unchanged_running_job_causes_zero_llm_invocation(self) -> None:
        self.log.write_text("startup\n", encoding="utf-8")
        spec = self.spec()
        state = cm.initial_monitor_state(spec, now_epoch=900)
        first, state = self.poll(state, spec, now=901)
        second, state = self.poll(state, spec, now=902)
        self.assertFalse(second["wake"])
        self.assertFalse(cm.event_matches(second, "wake"))
        self.assertNotIn("wake_packet", second)
        self.assertEqual(state["telemetry"]["luna_invocations"], 0)
        self.assertEqual(state["telemetry"]["sol_wakeups"], 0)
        self.assertGreaterEqual(first["telemetry"]["monitor_poll_count"], 1)

    def test_hundreds_of_unchanged_polls_cause_zero_llm_invocation(self) -> None:
        self.log.write_text("startup\n", encoding="utf-8")
        spec = self.spec()
        state = cm.initial_monitor_state(spec, now_epoch=0)
        for index in range(300):
            report, state = self.poll(state, spec, now=float(index + 1))
            self.assertFalse(report["wake"])
        telemetry = state["telemetry"]
        self.assertEqual(telemetry["monitor_poll_count"], 300)
        self.assertEqual(telemetry["luna_invocations"], 0)
        self.assertEqual(telemetry["sol_wakeups"], 0)
        self.assertEqual(telemetry["frontier_no_change_wakeups"], 0)

    def test_progress_only_append_causes_zero_llm_invocation(self) -> None:
        self.log.write_text("training_step=1 loss=2.0 grad_norm=0.7 throughput=100\n", encoding="utf-8")
        spec = self.spec()
        state = cm.initial_monitor_state(spec, now_epoch=0)
        _, state = self.poll(state, spec, now=1)
        with self.log.open("a", encoding="utf-8") as handle:
            handle.write("training_step=2 loss=1.9 grad_norm=0.8 throughput=101\n")
        report, state = self.poll(state, spec, now=2)
        self.assertFalse(report["wake"])
        self.assertEqual(state["last_step"], 2)
        self.assertEqual(state["telemetry"]["luna_invocations"], 0)
        self.assertEqual(state["telemetry"]["sol_wakeups"], 0)

    def test_exact_milestone_creates_one_wake_event(self) -> None:
        self.log.write_text("training_step=9 loss=2.0\n", encoding="utf-8")
        spec = self.spec(target_step=10)
        state = cm.initial_monitor_state(spec, now_epoch=0)
        _, state = self.poll(state, spec, now=1)
        with self.log.open("a", encoding="utf-8") as handle:
            handle.write("training_step=10 loss=1.8\n")
        report, state = self.poll(state, spec, now=2)
        self.assertTrue(report["wake"])
        self.assertEqual(report["route"], "SOL")
        self.assertEqual(report["wake_packet"]["wake_reason"], "MILESTONE")
        self.assertEqual(state["telemetry"]["sol_wakeups"], 1)

    def test_duplicate_milestone_does_not_wake_twice(self) -> None:
        self.log.write_text("training_step=10\n", encoding="utf-8")
        spec = self.spec(target_step=10)
        state = cm.initial_monitor_state(spec, now_epoch=0)
        first, state = self.poll(state, spec, now=1)
        second, state = self.poll(state, spec, now=2)
        self.assertTrue(first["wake"])
        self.assertFalse(second["wake"])
        self.assertEqual(state["telemetry"]["sol_wakeups"], 1)

    def test_one_shot_milestone_survives_bounded_dedupe_eviction(self) -> None:
        self.log.write_text("training_step=10\n", encoding="utf-8")
        spec = self.spec(target_step=10)
        state = cm.initial_monitor_state(spec, now_epoch=0)
        first, state = self.poll(state, spec, now=1)
        self.assertTrue(first["wake"])
        for step in range(11, 620):
            with self.log.open("a", encoding="utf-8") as handle:
                handle.write(f"training_step={step}\n")
            report, state = self.poll(state, spec, now=float(step))
            self.assertFalse(report["wake"])
        self.assertEqual(state["telemetry"]["sol_wakeups"], 1)
        self.assertLessEqual(len(state["dedupe"]), cm.MAX_DEDUPE_ENTRIES)

    def test_known_warning_follows_deterministic_route(self) -> None:
        self.log.write_text("KNOWN_CACHE_WARNING: using fallback cache\n", encoding="utf-8")
        spec = self.spec(known_warning_regex=["KNOWN_CACHE_WARNING"])
        state = cm.initial_monitor_state(spec, now_epoch=0)
        report, state = self.poll(state, spec, now=1)
        self.assertFalse(report["wake"])
        self.assertIn("KNOWN_WARNING", {event["event"] for event in report["events"]})
        self.assertEqual(state["telemetry"]["luna_invocations"], 0)

    def test_unknown_warning_invokes_luna_at_most_once_per_window(self) -> None:
        self.log.write_text("mysterious warning from scheduler plugin\n", encoding="utf-8")
        spec = self.spec()
        state = cm.initial_monitor_state(spec, now_epoch=0)
        first, state = self.poll(state, spec, now=1)
        self.assertEqual(first["route"], "LUNA")
        event_id = first["events"][0]["id"]
        _, state = cm.resolve_luna_event(
            state=state,
            spec=spec,
            response=self.routine_response(),
            event_id=event_id,
            now_epoch=2,
        )
        with self.log.open("a", encoding="utf-8") as handle:
            handle.write("mysterious warning from scheduler plugin\n")
        second, state = self.poll(state, spec, now=3)
        self.assertFalse(second["wake"])
        self.assertEqual(state["telemetry"]["luna_invocations"], 1)
        self.assertGreaterEqual(state["telemetry"]["events_deduplicated"], 1)

    def test_luna_receives_only_bounded_incremental_evidence(self) -> None:
        self.log.write_text("mysterious warning " + "x" * 50_000 + "\n", encoding="utf-8")
        spec = self.spec(luna_max_input_bytes=4096)
        state = cm.initial_monitor_state(spec, now_epoch=0)
        report, state = self.poll(state, spec, now=1, max_bytes=60_000)
        encoded = json.dumps(report["wake_packet"], separators=(",", ":")).encode("utf-8")
        self.assertLessEqual(len(encoded), 4096)
        self.assertNotIn("wake_conditions", report["wake_packet"]["event"].get("evidence", ""))
        self.assertEqual(state["telemetry"]["full_log_reads"], 0)

    def test_warning_burst_is_bounded_and_fails_closed(self) -> None:
        self.log.write_text(
            "".join(f"unfamiliar warning {index}\n" for index in range(300)),
            encoding="utf-8",
        )
        spec = self.spec()
        state = cm.initial_monitor_state(spec, now_epoch=0)
        report, state = self.poll(state, spec, now=1)
        self.assertEqual(report["route"], "SOL")
        self.assertLessEqual(len(report["events"]), 16)
        self.assertIn(
            "bounded_event_parser_overflow",
            json.dumps(report["wake_packet"], sort_keys=True),
        )
        self.assertEqual(state["telemetry"]["luna_invocations"], 0)

    def test_luna_routine_response_does_not_wake_sol(self) -> None:
        self.log.write_text("unfamiliar warning code 77\n", encoding="utf-8")
        spec = self.spec()
        state = cm.initial_monitor_state(spec, now_epoch=0)
        report, state = self.poll(state, spec, now=1)
        resolved, state = cm.resolve_luna_event(
            state=state,
            spec=spec,
            response=self.routine_response(),
            event_id=report["events"][0]["id"],
            input_tokens=100,
            output_tokens=20,
            now_epoch=2,
        )
        self.assertFalse(resolved["wake"])
        self.assertEqual(state["telemetry"]["sol_wakeups"], 0)
        self.assertEqual(state["telemetry"]["luna_input_tokens"], 100)
        self.assertEqual(state["telemetry"]["luna_output_tokens"], 20)

    def test_luna_frontier_required_response_wakes_sol(self) -> None:
        self.log.write_text("unfamiliar warning code 88\n", encoding="utf-8")
        spec = self.spec()
        state = cm.initial_monitor_state(spec, now_epoch=0)
        report, state = self.poll(state, spec, now=1)
        resolved, state = cm.resolve_luna_event(
            state=state,
            spec=spec,
            response=self.frontier_response(),
            event_id=report["events"][0]["id"],
            now_epoch=2,
        )
        self.assertTrue(resolved["wake"])
        self.assertEqual(resolved["route"], "SOL")
        self.assertEqual(state["telemetry"]["sol_wakeups"], 1)

    def test_multiple_ambiguous_events_are_triaged_without_loss(self) -> None:
        self.log.write_text(
            "unfamiliar warning alpha\nunfamiliar warning beta\n",
            encoding="utf-8",
        )
        spec = self.spec()
        state = cm.initial_monitor_state(spec, now_epoch=0)
        first, state = self.poll(state, spec, now=1)
        first_id = first["wake_packet"]["event"]["id"]
        resolved, state = cm.resolve_luna_event(
            state=state,
            spec=spec,
            response=self.routine_response(),
            event_id=first_id,
            now_epoch=2,
        )
        self.assertTrue(resolved["wake"])
        self.assertEqual(resolved["route"], "LUNA")
        second_id = resolved["wake_packet"]["event"]["id"]
        self.assertNotEqual(first_id, second_id)
        resolved, state = cm.resolve_luna_event(
            state=state,
            spec=spec,
            response=self.routine_response(),
            event_id=second_id,
            now_epoch=3,
        )
        self.assertFalse(resolved["wake"])
        self.assertEqual(state["telemetry"]["luna_invocations"], 2)
        self.assertEqual(state["telemetry"]["sol_wakeups"], 0)

    def test_scientific_event_bypasses_luna(self) -> None:
        self.log.write_text("PAIRED_CI_GATE_CONFLICT detected\n", encoding="utf-8")
        spec = self.spec(scientific_event_regex=["PAIRED_CI_GATE_CONFLICT"])
        state = cm.initial_monitor_state(spec, now_epoch=0)
        report, state = self.poll(state, spec, now=1)
        self.assertTrue(report["wake"])
        self.assertEqual(report["route"], "SOL")
        self.assertEqual(state["telemetry"]["luna_invocations"], 0)

    def test_log_rotation_resets_inode_cursor_without_losing_new_line(self) -> None:
        self.log.write_text("training_step=1\n", encoding="utf-8")
        spec = self.spec()
        state = cm.initial_monitor_state(spec, now_epoch=0)
        _, state = self.poll(state, spec, now=1)
        self.log.rename(self.root / "train.log.1")
        self.log.write_text("training_step=2\n", encoding="utf-8")
        report, state = self.poll(state, spec, now=2)
        self.assertTrue(report["jobs"][0]["log"]["rotated"])
        self.assertEqual(state["last_step"], 2)
        self.assertFalse(report["wake"])

    def test_log_truncation_resets_offset(self) -> None:
        self.log.write_text("training_step=100\n" + "x" * 100 + "\n", encoding="utf-8")
        spec = self.spec()
        state = cm.initial_monitor_state(spec, now_epoch=0)
        _, state = self.poll(state, spec, now=1)
        self.log.write_text("training_step=1\n", encoding="utf-8")
        report, state = self.poll(state, spec, now=2)
        self.assertTrue(report["jobs"][0]["log"]["truncated"])
        self.assertEqual(report["jobs"][0]["log"]["offset"], self.log.stat().st_size)
        self.assertFalse(report["wake"])

    def test_partial_final_line_is_parsed_only_after_completion(self) -> None:
        self.log.write_text("mysterious warn", encoding="utf-8")
        spec = self.spec()
        state = cm.initial_monitor_state(spec, now_epoch=0)
        first, state = self.poll(state, spec, now=1)
        self.assertFalse(first["wake"])
        with self.log.open("a", encoding="utf-8") as handle:
            handle.write("ing appeared\n")
        second, _ = self.poll(state, spec, now=2)
        self.assertEqual(second["route"], "LUNA")

    def test_monitor_restart_restores_cursor_state(self) -> None:
        self.log.write_text("training_step=1\n", encoding="utf-8")
        spec = self.spec()
        state = cm.initial_monitor_state(spec, now_epoch=0)
        _, state = self.poll(state, spec, now=1)
        state_file = self.root / "monitor-state.json"
        cm.save_state(state_file, state)
        restored = cm.load_state(state_file)
        prior_bytes = restored["telemetry"]["bytes_read_incrementally"]
        report, restored = self.poll(restored, spec, now=2)
        self.assertEqual(restored["telemetry"]["bytes_read_incrementally"], prior_bytes)
        self.assertEqual(report["jobs"][0]["log"]["new_bytes"], 0)

    def test_atomic_state_write_preserves_previous_file_after_interruption(self) -> None:
        path = self.root / "state.json"
        original = {"schema": 1, "jobs": {}}
        cm.save_state(path, original)
        with mock.patch.object(cm.os, "replace", side_effect=OSError("interrupted")):
            with self.assertRaises(cm.ClusterManagerError):
                cm.save_state(path, {"schema": 1, "jobs": {"123": {}}})
        self.assertEqual(json.loads(path.read_text(encoding="utf-8")), original)
        self.assertEqual(list(self.root.glob(".state.json.*.tmp")), [])

    def test_stalled_process_detection_wakes_sol(self) -> None:
        self.log.write_text("startup\n", encoding="utf-8")
        spec = self.spec(thresholds={"stall_seconds": 10, "log_stall_seconds": 0})
        state = cm.initial_monitor_state(spec, now_epoch=0)
        report, state = self.poll(state, spec, now=11)
        self.assertTrue(report["wake"])
        self.assertIn("STALL", {event["event"] for event in report["events"]})

    def test_failed_slurm_job_wakes_sol(self) -> None:
        self.log.write_text("startup\n", encoding="utf-8")
        spec = self.spec()
        state = cm.initial_monitor_state(spec, now_epoch=0)
        report, state = self.poll(
            state,
            spec,
            now=1,
            runner=FakeRunner(state="FAILED", exit_code="1:0"),
        )
        self.assertTrue(report["wake"])
        self.assertEqual(report["wake_packet"]["wake_reason"], "PROCESS_FAILED")

    def test_persistently_unknown_scheduler_state_is_not_waited_forever(self) -> None:
        self.log.write_text("startup\n", encoding="utf-8")
        spec = self.spec(thresholds={"scheduler_unknown_seconds": 300})
        state = cm.initial_monitor_state(spec, now_epoch=0)
        first, state = self.poll(state, spec, now=1, runner=FakeRunner(state="UNKNOWN"))
        self.assertFalse(first["wake"])
        second, state = self.poll(state, spec, now=302, runner=FakeRunner(state="UNKNOWN"))
        self.assertTrue(second["wake"])
        self.assertEqual(second["route"], "LUNA")

    def test_checkpoint_creation_wakes_once(self) -> None:
        checkpoint = self.root / "ckpt_10" / "manifest.json"
        self.log.write_text("training_step=9\n", encoding="utf-8")
        spec = self.spec(
            artifacts=[
                {
                    "path": str(checkpoint),
                    "kind": "checkpoint",
                    "wake_on_create": True,
                    "required_when": "never",
                }
            ]
        )
        state = cm.initial_monitor_state(spec, now_epoch=0)
        _, state = self.poll(state, spec, now=1)
        checkpoint.parent.mkdir()
        checkpoint.write_text('{"complete":true}\n', encoding="utf-8")
        first, state = self.poll(state, spec, now=2)
        second, state = self.poll(state, spec, now=3)
        self.assertTrue(first["wake"])
        self.assertEqual(first["wake_packet"]["wake_reason"], "CHECKPOINT")
        self.assertFalse(second["wake"])

    def test_checkpoint_directory_creation_wakes_once(self) -> None:
        checkpoint = self.root / "ckpt_10"
        self.log.write_text("training_step=9\n", encoding="utf-8")
        spec = self.spec(
            artifacts=[
                {
                    "path": str(checkpoint),
                    "kind": "checkpoint",
                    "wake_on_create": True,
                    "required_when": "never",
                }
            ]
        )
        state = cm.initial_monitor_state(spec, now_epoch=0)
        _, state = self.poll(state, spec, now=1)
        checkpoint.mkdir()
        first, state = self.poll(state, spec, now=2)
        second, _ = self.poll(state, spec, now=3)
        self.assertTrue(first["wake"])
        self.assertFalse(second["wake"])

    def test_completed_evaluation_wakes_sol(self) -> None:
        self.log.write_text("evaluation completed successfully\n", encoding="utf-8")
        spec = self.spec(phase="EVALUATION")
        state = cm.initial_monitor_state(spec, now_epoch=0)
        report, _ = self.poll(state, spec, now=1)
        self.assertTrue(report["wake"])
        self.assertEqual(report["wake_packet"]["wake_reason"], "EVAL_COMPLETED")

    def test_successive_checks_never_reread_entire_log(self) -> None:
        contents = "training_step=1\n" + "ordinary\n" * 100
        self.log.write_text(contents, encoding="utf-8")
        spec = self.spec()
        state = cm.initial_monitor_state(spec, now_epoch=0)
        _, state = self.poll(state, spec, now=1)
        first_bytes = state["telemetry"]["bytes_read_incrementally"]
        for tick in range(2, 20):
            _, state = self.poll(state, spec, now=float(tick))
        self.assertEqual(state["telemetry"]["bytes_read_incrementally"], first_bytes)
        self.assertEqual(state["telemetry"]["full_log_reads"], 0)

    def test_timer_is_not_a_frontier_wake_condition(self) -> None:
        self.log.write_text("startup\n", encoding="utf-8")
        spec_path = self.root / "spec.json"
        spec_path.write_text(json.dumps(self.spec()), encoding="utf-8")
        args = cm.build_parser().parse_args(
            [
                "watch",
                "123",
                "--monitor-spec",
                str(spec_path),
                "--timeout",
                "1",
                "--json",
            ]
        )
        with self.assertRaisesRegex(cm.ClusterManagerError, "finite timeout"):
            cm.run_watch(args)

    def test_monitor_state_contains_no_skill_or_handoff_transcript(self) -> None:
        spec = self.spec()
        state = cm.initial_monitor_state(spec, now_epoch=0)
        rendered = json.dumps(state)
        self.assertNotIn("SKILL.md", rendered)
        self.assertNotIn("conversation", rendered.lower())
        self.assertNotIn("assistant", rendered.lower())
        self.assertLess(len(rendered.encode("utf-8")), 10_000)

    def test_invalid_luna_response_cannot_make_a_silent_decision(self) -> None:
        self.log.write_text("unfamiliar warning code 99\n", encoding="utf-8")
        spec = self.spec()
        state = cm.initial_monitor_state(spec, now_epoch=0)
        report, state = self.poll(state, spec, now=1)
        with self.assertRaisesRegex(cm.ClusterManagerError, "classification"):
            cm.resolve_luna_event(
                state=state,
                spec=spec,
                response={"classification": "IGNORE"},
                event_id=report["events"][0]["id"],
                now_epoch=2,
            )
        self.assertIsNotNone(state["pending_luna"])

    def test_metric_debounce_counts_samples_not_empty_poll_cycles(self) -> None:
        self.log.write_text(
            "".join(f"training_step={index} loss=10\n" for index in range(1, 8)),
            encoding="utf-8",
        )
        spec = self.spec(
            thresholds={
                "minimum_metric_samples": 7,
                "consecutive_violations": 3,
                "loss_mad_z": 12.0,
            }
        )
        state = cm.initial_monitor_state(spec, now_epoch=0)
        _, state = self.poll(state, spec, now=1)
        for tick, loss in ((2, 100), (4, 101)):
            with self.log.open("a", encoding="utf-8") as handle:
                handle.write(f"training_step={tick + 6} loss={loss}\n")
            report, state = self.poll(state, spec, now=float(tick))
            self.assertFalse(report["wake"])
            report, state = self.poll(state, spec, now=float(tick + 1))
            self.assertFalse(report["wake"])
        with self.log.open("a", encoding="utf-8") as handle:
            handle.write("training_step=11 loss=102\n")
        report, state = self.poll(state, spec, now=6)
        self.assertTrue(report["wake"])
        self.assertIn("INVARIANT_FAILED", {event["event"] for event in report["events"]})

    def test_event_monitor_rejects_flags_that_can_create_no_change_turns(self) -> None:
        self.log.write_text("startup\n", encoding="utf-8")
        spec_path = self.root / "spec.json"
        spec_path.write_text(json.dumps(self.spec()), encoding="utf-8")
        unsafe = (
            ["--no-state"],
            ["--emit-initial"],
            ["--until", "progress"],
        )
        for flags in unsafe:
            with self.subTest(flags=flags):
                args = cm.build_parser().parse_args(
                    ["watch", "123", "--monitor-spec", str(spec_path), *flags, "--json"]
                )
                with self.assertRaises(cm.ClusterManagerError):
                    cm.run_watch(args)

    def test_monitor_state_has_a_single_process_owner(self) -> None:
        path = self.root / "state.json"
        with cm.monitor_state_lock(path):
            with self.assertRaisesRegex(cm.ClusterManagerError, "already owned"):
                with cm.monitor_state_lock(path):
                    self.fail("second watcher unexpectedly acquired the monitor state")

    def test_wake_evidence_is_durable_before_cursor_advance(self) -> None:
        self.log.write_text("training_step=10\n", encoding="utf-8")
        event_log = self.root / "events.jsonl"
        wake_file = self.root / "wake.json"
        spec = self.spec(
            target_step=10,
            event_log_path=str(event_log),
            wake_file=str(wake_file),
        )
        state_path = self.root / "state.json"
        old_state = cm.initial_monitor_state(spec, now_epoch=0)
        cm.save_state(state_path, old_state)
        report, next_state = self.poll(old_state, spec, now=1)
        with mock.patch.object(
            cm,
            "save_state",
            side_effect=cm.ClusterManagerError("simulated state interruption"),
        ):
            with self.assertRaisesRegex(cm.ClusterManagerError, "simulated"):
                cm.persist_monitor_outputs(
                    report,
                    state=next_state,
                    state_path=state_path,
                    spec=spec,
                    cwd=self.root,
                )
        self.assertTrue(wake_file.exists())
        self.assertIn(report["wake_packet"]["event_id"], event_log.read_text(encoding="utf-8"))
        restored = cm.load_state(state_path)
        replay, _ = self.poll(restored, spec, now=2)
        self.assertTrue(replay["wake"])
        self.assertEqual(replay["wake_packet"]["event_id"], report["wake_packet"]["event_id"])

    def test_restart_generation_retains_step_regression_detection(self) -> None:
        self.log.write_text("training_step=100 loss=10\n", encoding="utf-8")
        spec = self.spec()
        state = cm.initial_monitor_state(spec, now_epoch=0)
        _, state = self.poll(state, spec, now=1)
        self.log.write_text("training_step=1 loss=10\n", encoding="utf-8")
        first, state = self.poll(state, spec, now=2)
        self.assertFalse(first["wake"])
        self.assertEqual(state["jobs"]["123"]["log"]["semantic_generation"], 1)
        for tick, step in ((3, 2), (4, 3)):
            with self.log.open("a", encoding="utf-8") as handle:
                handle.write(f"training_step={step} loss=10\n")
            report, state = self.poll(state, spec, now=float(tick))
        self.assertTrue(report["wake"])
        self.assertIn("INVARIANT_FAILED", {event["event"] for event in report["events"]})


class ResearchMonitorHandoffTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        (self.root / ".git").mkdir()
        rm.init_research(self.root, "Test deterministic research monitoring.")
        spec_path = self.root / "run-spec.json"
        spec_path.write_text(json.dumps({
            "run_id": "exp-001",
            "hypothesis": "A bounded treatment improves score.",
            "change": "One controlled change.",
            "baseline": {"configuration": "control"},
            "treatment": {"configuration": "treatment"},
            "primary_metric": {"name": "score", "direction": "maximize"},
            "success_criteria": {"minimum_delta": 0.01},
            "failure_criteria": {"maximum_regression": -0.01},
            "evaluation": {"benchmark": "validation", "seeds": [1]},
        }), encoding="utf-8")
        rm.plan_run(self.root, spec_path)
        rm.validate_run(self.root, "exp-001", ["tests passed"], [])
        rm.record_launch(
            self.root,
            "exp-001",
            ["123"],
            ["logs/train.log"],
            ["outputs/model"],
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_arm_monitor_is_idempotent_and_stops_frontier_continuation(self) -> None:
        kwargs = {
            "phase": "TRAINING",
            "target_step": 200,
            "checkpoint_paths": ["outputs/ckpt_200/manifest.json"],
            "evaluation_complete_paths": [],
            "training_complete_paths": ["outputs/run_complete.json"],
            "known_warning_regex": ["benign cache warning"],
            "scientific_event_regex": ["paired gate conflict"],
            "stall_seconds": 1800,
            "log_stall_seconds": 1800,
            "next_scientific_action": "run health, provenance, reward, HAC, and Δ audits",
            "cluster_manager": "cluster-manager",
            "threshold_overrides": {
                "consecutive_violations": 4,
                "scheduler_unknown_seconds": 120.0,
            },
        }
        first = rm.arm_monitor(self.root, "exp-001", **kwargs)
        second = rm.arm_monitor(self.root, "exp-001", **kwargs)
        self.assertEqual(first["monitor"]["command"], second["monitor"]["command"])
        self.assertFalse(first["idempotent"])
        self.assertTrue(second["idempotent"])
        self.assertEqual(first["monitor"]["frontier_action"], "STOP_CONTINUATION")
        self.assertIn("--timeout", first["monitor"]["command"])
        self.assertIn("0", first["monitor"]["command"])
        self.assertIn("wait_agent", first["monitor"]["forbidden_while_waiting"])
        handoff = rm.build_handoff(self.root, 5)
        monitor = handoff["active_runs"][0]["monitor"]
        self.assertEqual(monitor["state_path"], ".research/monitors/exp-001/state.json")
        self.assertNotIn("command", monitor)
        self.assertLessEqual(len(rm.compact_json(handoff).encode("utf-8")), rm.MAX_HANDOFF_BYTES)
        spec = cm.load_monitor_spec(
            self.root / first["monitor"]["spec_path"],
            cwd=self.root,
        )
        state = cm.load_state(self.root / first["monitor"]["state_path"])
        prepared = cm.prepare_monitor_state(state, spec, now_epoch=1)
        self.assertEqual(prepared["spec_sha256"], cm.monitor_spec_fingerprint(spec))
        self.assertEqual(spec["thresholds"]["consecutive_violations"], 4)
        self.assertEqual(spec["thresholds"]["scheduler_unknown_seconds"], 120.0)
        monitor_events = [
            item
            for item in rm.tail_jsonl(self.root / ".research" / "runs.jsonl", 100)
            if item.get("event") == "monitor_armed"
        ]
        self.assertEqual(len(monitor_events), 1)


if __name__ == "__main__":
    unittest.main()
