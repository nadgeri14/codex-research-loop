from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts import research_manager as rm


def experiment_spec(run_id: str) -> dict:
    return {
        "run_id": run_id,
        "hypothesis": f"Hypothesis for {run_id}",
        "change": "Change exactly one controlled factor.",
        "baseline": {"configuration": "control"},
        "treatment": {"configuration": "treatment"},
        "primary_metric": {"name": "score", "direction": "maximize"},
        "success_criteria": {"minimum_delta": 0.01},
        "failure_criteria": {"maximum_regression": -0.01},
        "evaluation": {"benchmark": "validation", "seeds": [1, 2, 3]},
    }


def experiment_summary(*, anomaly: bool = False) -> dict:
    return {
        "status": "anomaly" if anomaly else "complete",
        "primary_metrics": {"score": {"value": 0.62, "std": 0.01}},
        "baseline_delta": {"score": 0.02},
        "sample_counts": {"examples": 100, "seeds": 3},
        "runtime": {"wall_seconds": 120, "gpu_hours": 0.03},
        "failure_modes": [],
        "anomalies": ["seed 2 variance spike"] if anomaly else [],
        "health_checks": {
            "nonfinite": "pass",
            "step_progress": "pass",
            "throughput": "pass",
            "distributed": "pass",
        },
        "evidence_paths": ["evidence/full-metrics.json", "logs/run.log"],
        "needs_judgment": True,
    }


class ResearchManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        (self.root / ".git").mkdir()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write_json(self, name: str, value: dict) -> Path:
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def initialize(self) -> None:
        rm.init_research(self.root, "Improve the benchmark without weakening scientific rigor.")

    def plan(self, run_id: str = "exp-001") -> dict:
        return rm.plan_run(self.root, self.write_json(f"{run_id}-spec.json", experiment_spec(run_id)))

    def prepare_launched_run(self, run_id: str = "exp-001") -> None:
        self.plan(run_id)
        rm.validate_run(self.root, run_id, ["unit and smoke tests passed"], ["reports/tests.txt"])
        rm.record_launch(self.root, run_id, ["64001"], ["logs/run.log"], ["outputs/model"])

    def test_init_is_idempotent_but_does_not_silently_replace_objective(self) -> None:
        first = rm.init_research(self.root, "Objective A")
        second = rm.init_research(self.root, "Objective A")
        self.assertTrue(first["created"])
        self.assertFalse(second["created"])
        with self.assertRaisesRegex(rm.ResearchManagerError, "different objective"):
            rm.init_research(self.root, "Objective B")

    def test_explicit_root_is_not_rebased_to_parent_git_repository(self) -> None:
        nested = self.root / "portable-install" / "project"
        self.assertEqual(rm.select_root(nested, self.root), nested.resolve())
        self.assertEqual(rm.select_root(None, nested), self.root.resolve())

    def test_spec_requires_a_predeclared_evaluation_and_safe_run_id(self) -> None:
        value = experiment_spec("exp-001")
        del value["evaluation"]
        with self.assertRaisesRegex(rm.ResearchManagerError, "evaluation"):
            rm.validate_spec(value)
        value = experiment_spec("../escape")
        with self.assertRaisesRegex(rm.ResearchManagerError, "invalid run id"):
            rm.validate_spec(value)

    def test_launch_requires_recorded_validation(self) -> None:
        self.initialize()
        self.plan()
        with self.assertRaisesRegex(rm.ResearchManagerError, "VALIDATED"):
            rm.record_launch(self.root, "exp-001", ["64001"], [], [])
        with self.assertRaisesRegex(rm.ResearchManagerError, "at least one --check"):
            rm.validate_run(self.root, "exp-001", [], [])
        validated = rm.validate_run(
            self.root,
            "exp-001",
            ["tests passed"],
            ["reports/test-output.txt"],
        )
        self.assertEqual(validated["status"], "VALIDATED")
        launched = rm.record_launch(
            self.root,
            "exp-001",
            ["64001"],
            ["logs/exp-001.log"],
            ["outputs/exp-001"],
        )
        self.assertEqual(launched["status"], "QUEUED")
        self.assertEqual(rm.load_state(self.root)["active_run_ids"], ["exp-001"])

    def test_invalid_job_id_is_rejected_without_shell_interpretation(self) -> None:
        self.initialize()
        self.plan()
        rm.validate_run(self.root, "exp-001", ["tests passed"], [])
        with self.assertRaisesRegex(rm.ResearchManagerError, "invalid Slurm job id"):
            rm.record_launch(self.root, "exp-001", ["64001;scancel 1"], [], [])

    def test_summary_preserves_evidence_paths_and_requires_scientific_decision(self) -> None:
        self.initialize()
        self.prepare_launched_run()
        summary_path = self.write_json("summary.json", experiment_summary())
        result = rm.record_summary(self.root, "exp-001", summary_path)
        self.assertEqual(result["status"], "READY")
        self.assertEqual(
            result["summary"]["evidence_paths"],
            ["evidence/full-metrics.json", "logs/run.log"],
        )
        self.assertTrue(result["summary"]["needs_judgment"])
        decision = rm.decide_run(
            self.root,
            "exp-001",
            "refine",
            "The mean improved, but confirmation is still required.",
            "Run a matched confirmation with new seeds.",
        )
        self.assertEqual(decision["decision"], "refine")
        self.assertEqual(rm.load_record(self.root, "exp-001")["status"], "DECIDED")
        self.assertNotIn("exp-001", rm.load_state(self.root)["active_run_ids"])

    def test_anomalies_cannot_be_marked_as_not_needing_judgment(self) -> None:
        value = experiment_summary(anomaly=True)
        value["needs_judgment"] = False
        with self.assertRaisesRegex(rm.ResearchManagerError, "must set needs_judgment"):
            rm.validate_summary(value)

    def test_sync_uses_one_compact_cluster_manager_call(self) -> None:
        self.initialize()
        self.prepare_launched_run()
        calls: list[list[str]] = []

        def runner(args):
            command = list(args)
            calls.append(command)
            output = {
                "schema": 1,
                "checked_at": "2026-08-14T12:00:00+00:00",
                "anomaly": False,
                "jobs": [{"job_id": "64001", "state": "COMPLETED"}],
            }
            return subprocess.CompletedProcess(command, 0, json.dumps(output), "")

        report = rm.sync_runs(
            self.root,
            ["exp-001"],
            cluster_manager="cluster-manager",
            runner=runner,
        )
        self.assertEqual(
            calls,
            [["cluster-manager", "status", "64001", "--no-state", "--json"]],
        )
        self.assertEqual(report["updates"][0]["status"], "REDUCING")
        self.assertTrue(report["updates"][0]["changed"])

    def test_compare_aligns_metrics_but_does_not_choose_a_winner(self) -> None:
        self.initialize()
        self.prepare_launched_run("exp-a")
        rm.record_summary(self.root, "exp-a", self.write_json("summary-a.json", experiment_summary()))

        self.prepare_launched_run("exp-b")
        second = experiment_summary()
        second["primary_metrics"]["score"]["value"] = 0.64
        second["baseline_delta"]["score"] = 0.04
        rm.record_summary(self.root, "exp-b", self.write_json("summary-b.json", second))

        comparison = rm.compare_runs(self.root, ["exp-a", "exp-b"])
        self.assertEqual(comparison["metric_names"], ["score"])
        self.assertEqual([row["metrics"]["score"] for row in comparison["runs"]], [0.62, 0.64])
        self.assertTrue(comparison["interpretation_required"])
        self.assertNotIn("winner", comparison)

    def test_handoff_is_bounded_and_raw_evidence_is_not_loaded(self) -> None:
        self.initialize()
        giant_evidence = self.root / "evidence" / "full-metrics.json"
        giant_evidence.parent.mkdir(parents=True)
        giant_evidence.write_text("SECRET_RAW_EVIDENCE" + "x" * 100_000, encoding="utf-8")
        (self.root / "logs").mkdir()
        (self.root / "logs" / "run.log").write_text("large raw log", encoding="utf-8")
        self.prepare_launched_run()
        rm.record_summary(self.root, "exp-001", self.write_json("summary.json", experiment_summary()))

        handoff = rm.build_handoff(self.root, 5)
        encoded = rm.compact_json(handoff).encode("utf-8")
        self.assertLessEqual(len(encoded), rm.MAX_HANDOFF_BYTES)
        rendered = encoded.decode("utf-8")
        self.assertIn("evidence/full-metrics.json", rendered)
        self.assertNotIn("SECRET_RAW_EVIDENCE", rendered)
        self.assertIn("bounded index", handoff["evidence_policy"])

    def test_large_metric_sets_are_reduced_without_losing_primary_evidence_index(self) -> None:
        self.initialize()
        self.prepare_launched_run()
        summary = experiment_summary()
        for index in range(500):
            name = f"secondary_{index:04d}"
            summary["primary_metrics"][name] = {"value": index / 1000, "detail": "x" * 400}
            summary["baseline_delta"][name] = index / 10_000
        rm.record_summary(self.root, "exp-001", self.write_json("large-summary.json", summary))

        handoff = rm.build_handoff(self.root, 5)
        comparison = rm.compare_runs(self.root, ["exp-001"])
        self.assertLessEqual(len(rm.compact_json(handoff).encode("utf-8")), rm.MAX_HANDOFF_BYTES)
        self.assertLessEqual(len(rm.compact_json(comparison).encode("utf-8")), rm.MAX_COMPARE_BYTES)
        self.assertEqual(comparison["runs"][0]["metrics"]["score"], 0.62)
        self.assertGreater(comparison["runs"][0]["omitted_metrics"], 0)
        inspected = rm.inspect_run(self.root, "exp-001", "summary")
        self.assertTrue(inspected["lossless_structured_retrieval"])
        self.assertEqual(len(inspected["summary"]["primary_metrics"]), 501)

    def test_training_health_detects_nonfinite_loss_and_preserves_raw_log(self) -> None:
        self.initialize()
        self.prepare_launched_run()
        rm.set_run_state(self.root, "exp-001", "RUNNING", "training started")
        log = self.root / "logs" / "run.log"
        log.parent.mkdir(parents=True)
        contents = (
            "global_step=10 loss=1.2 throughput=100 tokens/s\n"
            "global_step=11 loss=0.9 throughput=102 tokens/s\n"
            "global_step=12 loss=nan grad_norm=inf\n"
        )
        log.write_text(contents, encoding="utf-8")

        report = rm.training_health(self.root, "exp-001", stale_seconds=0)
        categories = {item["category"] for item in report["signals"]}
        self.assertFalse(report["healthy"])
        self.assertTrue(report["needs_judgment"])
        self.assertIn("nonfinite", categories)
        self.assertEqual(log.read_text(encoding="utf-8"), contents)
        self.assertEqual(rm.load_record(self.root, "exp-001")["health_status"], "critical")

        inspected = rm.inspect_run(self.root, "exp-001", "evidence")
        by_path = {item["path"]: item for item in inspected["evidence"]}
        self.assertTrue(by_path["logs/run.log"]["exists"])
        self.assertEqual(by_path["logs/run.log"]["size"], len(contents.encode("utf-8")))

    def test_training_health_detects_stalled_progress_without_declaring_failure(self) -> None:
        self.initialize()
        self.prepare_launched_run()
        rm.set_run_state(self.root, "exp-001", "RUNNING", "training started")
        log = self.root / "logs" / "run.log"
        log.parent.mkdir(parents=True)
        log.write_text("global_step=100 loss=0.5 throughput=100\n", encoding="utf-8")
        now = 2_000_000_000.0
        os.utime(log, (now - 3_600, now - 3_600))

        report = rm.training_health(
            self.root,
            "exp-001",
            stale_seconds=300,
            now_epoch=now,
        )
        categories = {item["category"] for item in report["signals"]}
        self.assertTrue(report["healthy"])
        self.assertTrue(report["needs_judgment"])
        self.assertEqual(report["status"], "warning")
        self.assertIn("stalled_progress", categories)

    def test_summary_cannot_hide_unresolved_training_health_signals(self) -> None:
        self.initialize()
        self.prepare_launched_run()
        rm.set_run_state(self.root, "exp-001", "RUNNING", "training started")
        log = self.root / "logs" / "run.log"
        log.parent.mkdir(parents=True)
        log.write_text("global_step=4 loss=NaN\n", encoding="utf-8")
        rm.training_health(self.root, "exp-001", stale_seconds=0)
        summary = experiment_summary()
        summary["needs_judgment"] = False
        with self.assertRaisesRegex(rm.ResearchManagerError, "unresolved training-health signals"):
            rm.record_summary(self.root, "exp-001", self.write_json("unsafe-summary.json", summary))

    def test_status_remains_bounded_with_many_long_active_hypotheses(self) -> None:
        self.initialize()
        for index in range(12):
            run_id = f"long-{index:02d}"
            spec = experiment_spec(run_id)
            spec["hypothesis"] = f"{run_id}:" + "h" * 7_500
            spec["change"] = "c" * 7_500
            rm.plan_run(self.root, self.write_json(f"{run_id}.json", spec))
        report = rm.status_report(self.root)
        self.assertEqual(report["active_run_count"], 12)
        self.assertEqual(report["omitted_active_runs"], 2)
        self.assertLessEqual(len(rm.compact_json(report).encode("utf-8")), rm.MAX_STATUS_BYTES)

    def test_doctor_warns_about_missing_evidence_without_reading_it(self) -> None:
        self.initialize()
        self.prepare_launched_run()
        report = rm.doctor(self.root)
        self.assertTrue(report["healthy"])
        self.assertTrue(any("currently missing" in warning for warning in report["warnings"]))

    def test_illegal_transition_is_rejected(self) -> None:
        self.initialize()
        self.plan()
        with self.assertRaisesRegex(rm.ResearchManagerError, "illegal run transition"):
            rm.set_run_state(self.root, "exp-001", "READY", "skip all evidence")


if __name__ == "__main__":
    unittest.main()
