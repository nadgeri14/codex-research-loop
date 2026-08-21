from __future__ import annotations

import argparse
import json
import stat
import tempfile
import unittest
from pathlib import Path

from scripts import install_codex_research_loop as installer


class CodexResearchLoopInstallerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.repo_root = Path(__file__).resolve().parents[1]
        self.codex_home = self.root / "home" / ".codex"
        self.skills_dir = self.root / "home" / ".agents" / "skills"
        self.bin_dir = self.root / "home" / ".local" / "bin"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def targets(self):
        return installer.build_targets(
            self.repo_root,
            codex_home=self.codex_home,
            skills_dir=self.skills_dir,
            bin_dir=self.bin_dir,
        )

    def test_dry_run_makes_no_changes(self) -> None:
        result = installer.install_targets(
            self.targets(), codex_home=self.codex_home, dry_run=True
        )
        self.assertEqual(result["status"], "dry-run")
        self.assertGreaterEqual(len(result["changed"]), 7)
        self.assertFalse(self.codex_home.exists())
        self.assertFalse(self.bin_dir.exists())
        self.assertFalse(self.skills_dir.exists())

    def test_install_is_global_idempotent_and_preserves_existing_agents_content(self) -> None:
        self.codex_home.mkdir(parents=True)
        agents = self.codex_home / "AGENTS.md"
        original = "# Existing global rules\n\n- Keep this exact rule.\n"
        agents.write_text(original, encoding="utf-8")

        first = installer.install_targets(
            self.targets(), codex_home=self.codex_home, dry_run=False
        )
        self.assertEqual(first["status"], "installed")
        installed_agents = agents.read_text(encoding="utf-8")
        self.assertTrue(installed_agents.startswith(original))
        self.assertEqual(installed_agents.count(installer.START_MARKER), 1)
        self.assertEqual(installed_agents.count(installer.END_MARKER), 1)

        research_manager = self.bin_dir / "research-manager"
        cluster_manager = self.bin_dir / "cluster-manager"
        self.assertTrue(research_manager.exists())
        self.assertTrue(cluster_manager.exists())
        self.assertEqual(stat.S_IMODE(research_manager.stat().st_mode), 0o755)
        self.assertEqual(stat.S_IMODE(cluster_manager.stat().st_mode), 0o755)
        self.assertTrue((self.skills_dir / "research-loop" / "SKILL.md").exists())
        package_root = installer.locate_package_root(self.repo_root)
        packaged_agents = {
            path.name for path in (package_root / "agents").glob("*.toml")
        }
        installed_agent_names = {
            path.name for path in (self.codex_home / "agents").glob("*.toml")
        }
        self.assertEqual(installed_agent_names, packaged_agents)
        self.assertEqual(
            packaged_agents,
            {
                "cluster-checker.toml",
                "cluster-monitor.toml",
                "research-code-reviewer.toml",
                "research-lead.toml",
            },
        )

        second = installer.install_targets(
            self.targets(), codex_home=self.codex_home, dry_run=False
        )
        self.assertEqual(second["status"], "up-to-date")
        self.assertEqual(second["changed"], [])
        self.assertEqual(agents.read_text(encoding="utf-8"), installed_agents)

    def test_update_creates_recoverable_backup_manifest(self) -> None:
        self.bin_dir.mkdir(parents=True)
        old_manager = self.bin_dir / "research-manager"
        old_manager.write_text("old manager\n", encoding="utf-8")
        old_manager.chmod(0o700)

        result = installer.install_targets(
            self.targets(), codex_home=self.codex_home, dry_run=False
        )
        backup_dir = Path(result["backup_dir"])
        manifest = json.loads((backup_dir / "manifest.json").read_text(encoding="utf-8"))
        entry = next(
            item for item in manifest["files"] if item["original"] == str(old_manager)
        )
        self.assertEqual(Path(entry["backup"]).read_text(encoding="utf-8"), "old manager\n")
        self.assertEqual(entry["mode"], 0o700)
        self.assertNotEqual(old_manager.read_text(encoding="utf-8"), "old manager\n")

    def test_managed_block_replacement_does_not_change_surrounding_text(self) -> None:
        before = "# Before\n\n"
        after = "\n\n# After\n\n- untouched\n"
        existing = (
            before
            + installer.START_MARKER
            + "\nold policy\n"
            + installer.END_MARKER
            + after
        )
        rendered = installer.render_managed_agents(existing, "# New policy\n\n- new")
        self.assertTrue(rendered.startswith(before))
        self.assertTrue(rendered.endswith(after))
        self.assertNotIn("old policy", rendered)
        self.assertIn("- new", rendered)
        self.assertEqual(rendered.count(installer.START_MARKER), 1)

    def test_legacy_policy_is_migrated_without_duplication(self) -> None:
        existing = (
            "# Storage safety\n\n- preserve\n\n"
            "# Cluster-wide research token policy\n\n- legacy\n"
        )
        rendered = installer.render_managed_agents(existing, "# New policy\n\n- current")
        self.assertIn("# Storage safety\n\n- preserve", rendered)
        self.assertNotIn("- legacy", rendered)
        self.assertEqual(rendered.count(installer.START_MARKER), 1)
        self.assertEqual(rendered.count("# New policy"), 1)

    def test_malformed_markers_are_rejected(self) -> None:
        with self.assertRaisesRegex(installer.InstallError, "malformed"):
            installer.render_managed_agents(
                f"existing\n{installer.START_MARKER}\n", "# policy"
            )

    def test_default_locations_use_official_user_skill_scope(self) -> None:
        args = argparse.Namespace(codex_home=None, skills_dir=None, bin_dir=None)
        home = self.root / "portable-home"
        codex_home, skills_dir, bin_dir = installer.default_locations(
            args, environ={}, home=home
        )
        self.assertEqual(codex_home, home / ".codex")
        self.assertEqual(skills_dir, home / ".agents" / "skills")
        self.assertEqual(bin_dir, home / ".local" / "bin")

    def test_obsolete_luna_implementer_is_reported_during_dry_run_without_removing(self) -> None:
        agents_dir = self.codex_home / "agents"
        agents_dir.mkdir(parents=True, exist_ok=True)
        obsolete = agents_dir / "research-implementer-luna.toml"
        obsolete.write_text("old luna implementer\n", encoding="utf-8")
        obsolete.chmod(0o644)
        unrelated = agents_dir / "custom-agent.toml"
        unrelated.write_text("keep me\n", encoding="utf-8")
        unrelated.chmod(0o644)

        result = installer.install_targets(
            self.targets(), codex_home=self.codex_home, dry_run=True
        )
        self.assertEqual(result.get("retired"), [str(obsolete)])
        self.assertEqual(result.get("removed"), [])
        self.assertIn(
            {"label": "retire:research-implementer-luna.toml", "action": "remove", "path": str(obsolete)},
            result.get("changed", []),
        )
        self.assertTrue(obsolete.exists())
        self.assertEqual(obsolete.read_text(encoding="utf-8"), "old luna implementer\n")
        self.assertTrue(unrelated.exists())
        self.assertEqual(unrelated.read_text(encoding="utf-8"), "keep me\n")
        self.assertIsNone(result.get("backup_dir"))
        self.assertFalse((self.codex_home / "backups").exists())

    def test_install_backs_up_and_removes_obsolete_luna_while_preserving_unrelated(self) -> None:
        agents_dir = self.codex_home / "agents"
        agents_dir.mkdir(parents=True, exist_ok=True)
        obsolete = agents_dir / "research-implementer-luna.toml"
        obsolete.write_text("old luna content\n", encoding="utf-8")
        obsolete.chmod(0o600)
        unrelated = agents_dir / "custom-agent.toml"
        unrelated.write_text("keep me\n", encoding="utf-8")
        unrelated.chmod(0o644)

        result = installer.install_targets(
            self.targets(), codex_home=self.codex_home, dry_run=False
        )
        self.assertEqual(result.get("retired"), [str(obsolete)])
        self.assertEqual(result.get("removed"), [str(obsolete)])
        self.assertIn(
            {"label": "retire:research-implementer-luna.toml", "action": "remove", "path": str(obsolete)},
            result.get("changed", []),
        )
        # Must have backed up and removed only the exact obsolete file
        self.assertFalse(obsolete.exists())
        self.assertTrue(unrelated.exists())
        self.assertEqual(unrelated.read_text(encoding="utf-8"), "keep me\n")
        self.assertIsNotNone(result.get("backup_dir"))
        backup_dir = Path(result["backup_dir"])  # type: ignore[arg-type]
        self.assertTrue(backup_dir.is_dir())
        manifest = json.loads((backup_dir / "manifest.json").read_text(encoding="utf-8"))
        entry = next(
            item for item in manifest["files"] if item["original"] == str(obsolete)
        )
        self.assertEqual(Path(entry["backup"]).read_text(encoding="utf-8"), "old luna content\n")
        self.assertEqual(entry["mode"], 0o600)
        # Packaged agents must still be installed
        package_root = installer.locate_package_root(self.repo_root)
        packaged_agents = {path.name for path in (package_root / "agents").glob("*.toml")}
        installed_agents = {path.name for path in (self.codex_home / "agents").glob("*.toml")}
        self.assertIn("research-lead.toml", installed_agents)
        self.assertNotIn("research-implementer-luna.toml", installed_agents)
        self.assertNotIn("research-implementer-luna.toml", packaged_agents)

    def test_retirement_is_idempotent_and_never_prunes_unrelated_agents(self) -> None:
        agents_dir = self.codex_home / "agents"
        agents_dir.mkdir(parents=True, exist_ok=True)
        obsolete = agents_dir / "research-implementer-luna.toml"
        obsolete.write_text("old\n", encoding="utf-8")
        unrelated = agents_dir / "custom-agent.toml"
        unrelated.write_text("keep\n", encoding="utf-8")

        first = installer.install_targets(
            self.targets(), codex_home=self.codex_home, dry_run=False
        )
        self.assertEqual(first.get("retired"), [str(obsolete)])
        self.assertEqual(first.get("removed"), [str(obsolete)])
        self.assertIn(
            {"label": "retire:research-implementer-luna.toml", "action": "remove", "path": str(obsolete)},
            first.get("changed", []),
        )
        self.assertFalse(obsolete.exists())
        self.assertTrue(unrelated.exists())

        second = installer.install_targets(
            self.targets(), codex_home=self.codex_home, dry_run=False
        )
        self.assertEqual(second["status"], "up-to-date")
        self.assertEqual(second["changed"], [])
        self.assertEqual(second.get("retired"), [])
        self.assertEqual(second.get("removed"), [])
        self.assertIsNone(second.get("backup_dir"))
        self.assertFalse(obsolete.exists())
        self.assertTrue(unrelated.exists())
        self.assertEqual(unrelated.read_text(encoding="utf-8"), "keep\n")

        dry_after = installer.install_targets(
            self.targets(), codex_home=self.codex_home, dry_run=True
        )
        self.assertEqual(dry_after.get("retired"), [])
        self.assertEqual(dry_after.get("removed"), [])
        self.assertNotIn(
            {"label": "retire:research-implementer-luna.toml", "action": "remove", "path": str(obsolete)},
            dry_after.get("changed", []),
        )

    def test_portable_payload_has_no_original_server_path(self) -> None:
        package_root = installer.locate_package_root(self.repo_root)
        payloads = [
            self.repo_root / "scripts" / "cluster_manager.py",
            self.repo_root / "scripts" / "research_manager.py",
            self.repo_root / "scripts" / "install_codex_research_loop.py",
            package_root / ".gitignore",
            package_root / "AGENTS.block.md",
            package_root / "README.md",
            *(
                path
                for directory in (package_root / "agents", package_root / "skill")
                for path in directory.rglob("*")
                if path.is_file()
            ),
        ]
        rendered = "\n".join(
            path.read_text(encoding="utf-8", errors="replace") for path in payloads
        )
        self.assertNotIn("/" + "lustre" + "/" + "nvwulf", rendered)
        self.assertNotIn("ana" + "dgeri", rendered)


if __name__ == "__main__":
    unittest.main()
