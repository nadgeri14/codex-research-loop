from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class PortableDistributionTests(unittest.TestCase):
    def test_all_native_agent_profiles_are_present(self) -> None:
        self.assertEqual(
            {path.name for path in (ROOT / "agents").glob("*.toml")},
            {
                "cluster-checker.toml",
                "cluster-monitor.toml",
                "muse-implementor.toml",
                "research-code-reviewer.toml",
                "research-lead.toml",
            },
        )

    def test_muse_profile_uses_the_native_router_model(self) -> None:
        profile = (ROOT / "agents" / "muse-implementor.toml").read_text(
            encoding="utf-8"
        )
        self.assertIn('name = "muse_implementor"', profile)
        self.assertIn('model = "meta/muse-spark-1.2-contributor"', profile)
        self.assertIn('model_reasoning_effort = "xhigh"', profile)
        self.assertIn('sandbox_mode = "workspace-write"', profile)
        self.assertIn("developer_instructions", profile)

    def test_obsolete_direct_muse_lanes_are_absent(self) -> None:
        self.assertFalse((ROOT / "scripts" / "muse_research_worker.py").exists())
        self.assertFalse((ROOT / "scripts" / "muse_cli_worker.py").exists())
        self.assertFalse((ROOT / "tests" / "test_muse_research_worker.py").exists())
        self.assertFalse((ROOT / "tests" / "test_muse_cli_worker.py").exists())

    def test_portable_tree_contains_no_original_machine_paths(self) -> None:
        # Assemble these at runtime so the guard does not trigger on itself.
        prohibited = ("/" + "lustre/", "anad" + "geri", "_" + "codex-router/")
        for path in ROOT.rglob("*"):
            if (
                not path.is_file()
                or ".git" in path.parts
                or "__pycache__" in path.parts
                or path.suffix in {".pyc", ".pyo"}
            ):
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            for value in prohibited:
                self.assertNotIn(value, text, f"{value!r} leaked into {path}")

    def test_router_patch_carries_code_and_regression_test(self) -> None:
        patch = (ROOT / "patches" / "codex-router-meta-agent-message.patch").read_text(
            encoding="utf-8"
        )
        self.assertIn("function normalizeMetaAgentMessages(input)", patch)
        self.assertIn(
            "API forwarder converts Codex agent_message input for Meta Responses",
            patch,
        )


if __name__ == "__main__":
    unittest.main()
