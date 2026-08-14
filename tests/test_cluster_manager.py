from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts import cluster_manager as cm


class FakeRunner:
    def __init__(self, outputs: dict[str, str | tuple[int, str, str]]) -> None:
        self.outputs = outputs
        self.calls: list[list[str]] = []

    def __call__(self, args):
        command = list(args)
        self.calls.append(command)
        value = self.outputs.get(tuple(command), self.outputs.get(command[0], ""))
        if isinstance(value, tuple):
            returncode, stdout, stderr = value
        else:
            returncode, stdout, stderr = 0, value, ""
        return subprocess.CompletedProcess(command, returncode, stdout, stderr)


class ClusterManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_parse_squeue(self) -> None:
        jobs = cm.parse_squeue("64001|train|RUNNING|00:12|00:48|1|b40x4-01|2026-08-14T12:00:00\n")
        self.assertEqual(
            jobs,
            [
                cm.JobRecord(
                    job_id="64001",
                    name="train",
                    state="RUNNING",
                    elapsed="00:12",
                    time_left="00:48",
                    nodes="1",
                    location="b40x4-01",
                    start="2026-08-14T12:00:00",
                )
            ],
        )

    def test_query_jobs_falls_back_to_sacct(self) -> None:
        runner = FakeRunner(
            {
                "squeue": (1, "", "slurm_load_jobs error: Invalid job id specified"),
                "sacct": "64001|train|COMPLETED|00:19:03|0:0|b40x4-01|start|end\n",
            }
        )
        jobs, warnings = cm.query_jobs(["64001"], user="alice", runner=runner)
        self.assertEqual(warnings, [])
        self.assertEqual(jobs[0].state, "COMPLETED")
        self.assertTrue(jobs[0].terminal)
        self.assertFalse(jobs[0].failed)
        self.assertEqual([call[0] for call in runner.calls], ["squeue", "sacct"])

    def test_log_scan_is_delta_only_and_detects_milestones(self) -> None:
        log = self.tmp_path / "run_64001.log"
        log.write_text("startup\ntraining_step: 1\n")
        milestones = cm.compile_patterns(cm.DEFAULT_MILESTONE_PATTERNS, [])
        errors = cm.compile_patterns(cm.DEFAULT_ERROR_PATTERNS, [])

        first, state = cm.scan_log(
            log,
            previous={},
            cwd=self.tmp_path,
            max_bytes=10_000,
            milestone_patterns=milestones,
            error_patterns=errors,
        )
        self.assertEqual(first["milestones"], ["training_step: 1"])

        with log.open("a") as handle:
            handle.write("ordinary progress\ntraining_step: 2\n")
        second, second_state = cm.scan_log(
            log,
            previous=state,
            cwd=self.tmp_path,
            max_bytes=10_000,
            milestone_patterns=milestones,
            error_patterns=errors,
        )
        self.assertEqual(second["milestones"], ["training_step: 2"])
        self.assertNotIn("training_step: 1", "\n".join(second["milestones"]))

        with log.open("a") as handle:
            handle.write("training_step: 2\n")
        third, _ = cm.scan_log(
            log,
            previous=second_state,
            cwd=self.tmp_path,
            max_bytes=10_000,
            milestone_patterns=milestones,
            error_patterns=errors,
        )
        self.assertNotIn("milestones", third)

    def test_report_suppresses_unchanged_scheduler_and_log_state(self) -> None:
        log = self.tmp_path / "run_64001.log"
        log.write_text("startup only\n")
        runner = FakeRunner(
            {"squeue": "64001|train|RUNNING|00:12|00:48|1|node01|start\n"}
        )
        kwargs = {
            "user": "alice",
            "cwd": self.tmp_path,
            "scan_logs": True,
            "explicit_logs": {"64001": log},
            "auto_log": False,
            "max_log_bytes": 10_000,
            "milestone_patterns": cm.compile_patterns(cm.DEFAULT_MILESTONE_PATTERNS, []),
            "error_patterns": cm.compile_patterns(cm.DEFAULT_ERROR_PATTERNS, []),
            "runner": runner,
        }
        first, state = cm.build_report(["64001"], state={"schema": 1, "jobs": {}}, **kwargs)
        second, _ = cm.build_report(["64001"], state=state, **kwargs)
        self.assertTrue(first["changed"])
        self.assertFalse(second["changed"])
        self.assertEqual(second["jobs"][0]["log"]["new_bytes"], 0)

    def test_failed_job_is_an_anomaly(self) -> None:
        runner = FakeRunner(
            {
                "squeue": "",
                "sacct": "64001|train|OUT_OF_MEMORY|00:01:03|0:125|node01|start|end\n",
            }
        )
        jobs, _ = cm.query_jobs(["64001"], user="alice", runner=runner)
        self.assertTrue(jobs[0].terminal)
        self.assertTrue(jobs[0].failed)

    def test_default_log_patterns_surface_training_failures(self) -> None:
        patterns = cm.compile_patterns(cm.DEFAULT_ERROR_PATTERNS, [])
        lines = [
            "global_step=120 loss=NaN grad_norm=inf",
            "ProcessGroupNCCL watchdog timed out during collective",
            "DataLoader worker 7 exited unexpectedly",
            "ordinary training progress",
        ]
        matches = cm.matching_lines(lines, patterns, limit=10)
        self.assertEqual(matches, lines[:3])

    def test_nonfinite_training_log_marks_live_job_as_anomaly(self) -> None:
        log = self.tmp_path / "run_64001.log"
        log.write_text("global_step=5 loss=nan\n")
        runner = FakeRunner(
            {"squeue": "64001|train|RUNNING|00:12|00:48|1|node01|start\n"}
        )
        report, _ = cm.build_report(
            ["64001"],
            user="alice",
            cwd=self.tmp_path,
            state={"schema": 1, "jobs": {}},
            scan_logs=True,
            explicit_logs={"64001": log},
            auto_log=False,
            max_log_bytes=10_000,
            milestone_patterns=cm.compile_patterns(cm.DEFAULT_MILESTONE_PATTERNS, []),
            error_patterns=cm.compile_patterns(cm.DEFAULT_ERROR_PATTERNS, []),
            runner=runner,
        )
        self.assertTrue(report["anomaly"])
        self.assertIn("loss=nan", report["jobs"][0]["log"]["errors"][0].lower())

    def test_invalid_job_id_is_rejected(self) -> None:
        with self.assertRaisesRegex(cm.ClusterManagerError, "invalid Slurm job id"):
            cm.validate_job_ids(["64001;scancel 1"])

    def test_resources_are_aggregated_by_partition(self) -> None:
        runner = FakeRunner(
            {
                "sinfo": (
                    "gpu|up|8:00:00|2|mix|gpu:h200:4|node[1-2]\n"
                    "gpu|up|8:00:00|1|alloc|gpu:h200:4|node3\n"
                )
            }
        )
        report = cm.query_resources(runner=runner)
        self.assertEqual(
            report["partitions"],
            [
                {
                    "partition": "gpu",
                    "availability": "up",
                    "time_limit": "8:00:00",
                    "gres": "gpu:h200:4",
                    "nodes": 3,
                    "states": {"mix": 2, "alloc": 1},
                }
            ],
        )

    def test_gpu_nodes_report_free_capacity(self) -> None:
        output = (
            "NodeName=h200x4-01 State=MIXED Gres=gpu:h200:4(S:1) "
            "Partitions=debug-h200x4,h200x4 CfgTRES=cpu=96,gres/gpu=4,gres/gpu:h200=4 "
            "AllocTRES=cpu=24,gres/gpu=1,gres/gpu:h200=1\n"
        )
        self.assertEqual(
            cm.parse_gpu_nodes(output),
            [
                {
                    "node": "h200x4-01",
                    "gpu_type": "h200",
                    "state": "MIXED",
                    "total": 4,
                    "allocated": 1,
                    "free": 3,
                    "partitions": ["debug-h200x4", "h200x4"],
                }
            ],
        )

    def test_gpu_availability_excludes_reserved_and_draining_nodes(self) -> None:
        scontrol = (
            "NodeName=gpu01 State=IDLE Gres=gpu:h200:4 Partitions=gpu "
            "CfgTRES=cpu=96,gres/gpu=4 AllocTRES=\n"
            "NodeName=gpu02 State=MIXED+DRAIN Gres=gpu:h200:4 Partitions=gpu "
            "CfgTRES=cpu=96,gres/gpu=4 AllocTRES=cpu=1,gres/gpu=1\n"
            "NodeName=gpu03 State=MIXED Gres=gpu:h200:4 Partitions=gpu "
            "CfgTRES=cpu=96,gres/gpu=4 AllocTRES=cpu=1,gres/gpu=2\n"
        )
        runner = FakeRunner(
            {
                ("scontrol", "show", "nodes", "-o"): scontrol,
                ("sinfo", "-N", "-h", "-o", "%N|%T"): (
                    "gpu01|reserved\n"
                    "gpu02|draining\n"
                    "gpu03|mixed\n"
                ),
            }
        )
        report = cm.query_gpus(available_only=True, runner=runner)
        self.assertEqual([node["node"] for node in report["nodes"]], ["gpu03"])
        self.assertTrue(report["nodes"][0]["schedulable"])

    def test_terminal_state_ends_every_watch_condition(self) -> None:
        report = {"jobs": [{"terminal": True, "anomaly": False}]}
        for condition in ("event", "state", "milestone", "terminal", "anomaly"):
            with self.subTest(condition=condition):
                self.assertTrue(cm.event_matches(report, condition))


if __name__ == "__main__":
    unittest.main()
