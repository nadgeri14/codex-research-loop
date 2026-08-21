#!/usr/bin/env python3
"""Install the portable Codex research loop without clobbering user config."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import stat
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence


START_MARKER = "<!-- codex-research-loop:start -->"
END_MARKER = "<!-- codex-research-loop:end -->"
LEGACY_POLICY_HEADINGS = (
    "# Cluster-wide research token policy",
    "# Token-efficient research loop",
)

OBSOLETE_AGENT = "research-implementer-luna.toml"
OBSOLETE_LABEL = f"retire:{OBSOLETE_AGENT}"


class InstallError(RuntimeError):
    """A concise, user-facing installation error."""


@dataclass(frozen=True)
class Target:
    label: str
    destination: Path
    content: bytes
    mode: int


def normalized_path(value: str | Path) -> Path:
    return Path(value).expanduser().resolve(strict=False)


def require_file(path: Path) -> bytes:
    try:
        return path.read_bytes()
    except OSError as exc:
        raise InstallError(f"cannot read package file {path}: {exc}") from exc


def locate_package_root(repo_root: Path) -> Path:
    repo_root = normalized_path(repo_root)
    candidates = (repo_root, repo_root / "codex-research-loop")
    for candidate in candidates:
        if (
            (candidate / "AGENTS.block.md").is_file()
            and (candidate / "agents" / "cluster-monitor.toml").is_file()
            and (candidate / "skill" / "research-loop" / "SKILL.md").is_file()
        ):
            return candidate
    raise InstallError(
        f"cannot find the packaged research loop under {repo_root} or "
        f"{repo_root / 'codex-research-loop'}"
    )


def render_managed_agents(existing: str, policy: str) -> str:
    """Insert or replace only this package's marked AGENTS.md block."""

    if START_MARKER in policy or END_MARKER in policy:
        raise InstallError("the packaged policy must not contain installer markers")

    start_count = existing.count(START_MARKER)
    end_count = existing.count(END_MARKER)
    if start_count != end_count or start_count > 1:
        raise InstallError(
            "AGENTS.md has malformed codex-research-loop markers; repair them before installing"
        )

    managed = f"{START_MARKER}\n{policy.strip()}\n{END_MARKER}"
    if start_count == 1:
        start = existing.index(START_MARKER)
        end = existing.index(END_MARKER, start) + len(END_MARKER)
        return existing[:start] + managed + existing[end:]

    for heading in LEGACY_POLICY_HEADINGS:
        match = re.search(rf"(?m)^{re.escape(heading)}[ \t]*$", existing)
        if not match:
            continue
        next_heading = re.search(r"(?m)^# [^#\n].*$", existing[match.end() :])
        end = match.end() + next_heading.start() if next_heading else len(existing)
        before = existing[: match.start()]
        after = existing[end:]
        prefix = before if not before or before.endswith("\n\n") else before.rstrip("\n") + "\n\n"
        suffix = after if not after or after.startswith("\n") else "\n\n" + after
        return prefix + managed + suffix

    if not existing:
        return managed + "\n"
    separator = "" if existing.endswith("\n\n") else ("\n" if existing.endswith("\n") else "\n\n")
    return existing + separator + managed + "\n"


def build_targets(
    repo_root: Path,
    *,
    codex_home: Path,
    skills_dir: Path,
    bin_dir: Path,
) -> list[Target]:
    repo_root = normalized_path(repo_root)
    package_root = locate_package_root(repo_root)
    skill_source = package_root / "skill" / "research-loop"

    targets = [
        Target(
            "research-manager",
            bin_dir / "research-manager",
            require_file(repo_root / "scripts" / "research_manager.py"),
            0o755,
        ),
        Target(
            "cluster-manager",
            bin_dir / "cluster-manager",
            require_file(repo_root / "scripts" / "cluster_manager.py"),
            0o755,
        ),
    ]

    if not skill_source.is_dir():
        raise InstallError(f"missing packaged skill directory: {skill_source}")
    for source in sorted(skill_source.rglob("*")):
        if source.is_file():
            relative = source.relative_to(skill_source)
            targets.append(
                Target(
                    f"skill/{relative.as_posix()}",
                    skills_dir / "research-loop" / relative,
                    require_file(source),
                    stat.S_IMODE(source.stat().st_mode) or 0o644,
                )
            )

    agent_sources = sorted((package_root / "agents").glob("*.toml"))
    if not agent_sources:
        raise InstallError(f"missing packaged agent definitions: {package_root / 'agents'}")
    for source in agent_sources:
        targets.append(
            Target(
                f"agent/{source.name}",
                codex_home / "agents" / source.name,
                require_file(source),
                0o644,
            )
        )

    agents_path = codex_home / "AGENTS.md"
    if agents_path.is_symlink():
        raise InstallError(f"refusing to replace symlinked target: {agents_path}")
    try:
        existing_agents = agents_path.read_text(encoding="utf-8") if agents_path.exists() else ""
    except (OSError, UnicodeDecodeError) as exc:
        raise InstallError(f"cannot read {agents_path} as UTF-8: {exc}") from exc
    policy = require_file(package_root / "AGENTS.block.md").decode("utf-8")
    targets.append(
        Target(
            "global-AGENTS.md",
            agents_path,
            render_managed_agents(existing_agents, policy).encode("utf-8"),
            0o644,
        )
    )

    seen: set[Path] = set()
    for target in targets:
        destination = normalized_path(target.destination)
        if destination in seen:
            raise InstallError(f"duplicate installation target: {destination}")
        seen.add(destination)
    return targets


def target_action(target: Target) -> str:
    path = target.destination
    if path.is_symlink():
        raise InstallError(f"refusing to replace symlinked target: {path}")
    if not path.exists():
        return "create"
    if not path.is_file():
        raise InstallError(f"installation target is not a regular file: {path}")
    try:
        content_matches = path.read_bytes() == target.content
        mode_matches = stat.S_IMODE(path.stat().st_mode) == target.mode
    except OSError as exc:
        raise InstallError(f"cannot inspect installation target {path}: {exc}") from exc
    return "unchanged" if content_matches and mode_matches else "update"


def atomic_write(path: Path, content: bytes, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    except OSError:
        temporary.unlink(missing_ok=True)
        raise


def safe_backup_label(index: int, label: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "-", label).strip("-") or "target"
    return f"{index:02d}-{sanitized}"


def install_targets(
    targets: Sequence[Target],
    *,
    codex_home: Path,
    dry_run: bool,
) -> dict[str, object]:
    actions = [(target, target_action(target)) for target in targets]
    changed = [(target, action) for target, action in actions if action != "unchanged"]
    obsolete_path = codex_home / "agents" / OBSOLETE_AGENT
    retirement_needed = False
    if obsolete_path.is_symlink():
        raise InstallError(f"refusing to replace symlinked target: {obsolete_path}")
    elif obsolete_path.exists():
        if not obsolete_path.is_file():
            raise InstallError(f"installation target is not a regular file: {obsolete_path}")
        retirement_needed = True

    reported_changed = [
        {"label": target.label, "action": action, "path": str(target.destination)}
        for target, action in changed
    ]
    if retirement_needed:
        reported_changed.append(
            {"label": OBSOLETE_LABEL, "action": "remove", "path": str(obsolete_path)}
        )

    result: dict[str, object] = {
        "schema": 1,
        "status": "dry-run" if dry_run else ("installed" if (changed or retirement_needed) else "up-to-date"),
        "changed": reported_changed,
        "unchanged": [
            str(target.destination) for target, action in actions if action == "unchanged"
        ],
        "backup_dir": None,
        "retired": [str(obsolete_path)] if retirement_needed else [],
        "removed": [],
    }
    if dry_run or (not changed and not retirement_needed):
        return result

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    backup_dir = codex_home / "backups" / "research-loop" / f"{timestamp}-{os.getpid()}"
    existing = [(target, action) for target, action in changed if action == "update"]
    manifest_entries: list[dict[str, object]] = []
    needs_backup = bool(existing or retirement_needed)
    if needs_backup:
        backup_dir.mkdir(parents=True, exist_ok=False)
        for index, (target, _) in enumerate(existing, start=1):
            backup_path = backup_dir / safe_backup_label(index, target.label)
            try:
                shutil.copy2(target.destination, backup_path)
            except OSError as exc:
                raise InstallError(f"cannot back up {target.destination}: {exc}") from exc
            manifest_entries.append(
                {
                    "original": str(target.destination),
                    "backup": str(backup_path),
                    "mode": stat.S_IMODE(target.destination.stat().st_mode),
                }
            )
        if retirement_needed:
            backup_index = len(existing) + 1
            backup_path = backup_dir / safe_backup_label(backup_index, OBSOLETE_LABEL)
            try:
                shutil.copy2(obsolete_path, backup_path)
            except OSError as exc:
                raise InstallError(f"cannot back up {obsolete_path}: {exc}") from exc
            manifest_entries.append(
                {
                    "original": str(obsolete_path),
                    "backup": str(backup_path),
                    "mode": stat.S_IMODE(obsolete_path.stat().st_mode),
                }
            )
        atomic_write(
            backup_dir / "manifest.json",
            (json.dumps({"files": manifest_entries}, indent=2, sort_keys=True) + "\n").encode(),
            0o600,
        )
        result["backup_dir"] = str(backup_dir)

    try:
        for target, _ in changed:
            atomic_write(target.destination, target.content, target.mode)
    except OSError as exc:
        raise InstallError(f"cannot install {target.destination}: {exc}") from exc

    if retirement_needed:
        try:
            obsolete_path.unlink()
        except OSError as exc:
            raise InstallError(f"cannot remove obsolete agent {obsolete_path}: {exc}") from exc
        result["removed"] = [str(obsolete_path)]

    return result


def path_warnings(
    *,
    codex_home: Path,
    skills_dir: Path,
    bin_dir: Path,
    environ: dict[str, str],
) -> list[str]:
    warnings: list[str] = []
    path_entries = {
        str(normalized_path(entry))
        for entry in environ.get("PATH", "").split(os.pathsep)
        if entry
    }
    if str(normalized_path(bin_dir)) not in path_entries:
        warnings.append(f"{bin_dir} is not on PATH; add it before starting Codex")

    override = codex_home / "AGENTS.override.md"
    try:
        override_active = override.exists() and bool(override.read_text(encoding="utf-8").strip())
    except (OSError, UnicodeDecodeError):
        override_active = override.exists()
    if override_active:
        warnings.append(
            f"{override} is non-empty, so Codex will ignore {codex_home / 'AGENTS.md'} "
            "until the override is removed"
        )

    legacy_skill = codex_home / "skills" / "research-loop"
    if normalized_path(legacy_skill) != normalized_path(skills_dir / "research-loop") and legacy_skill.exists():
        warnings.append(
            f"legacy skill copy exists at {legacy_skill}; remove or disable it to avoid duplicate "
            "research-loop entries"
        )
    return warnings


def default_locations(
    args: argparse.Namespace,
    *,
    environ: dict[str, str],
    home: Path,
) -> tuple[Path, Path, Path]:
    codex_home = normalized_path(
        args.codex_home or environ.get("CODEX_HOME") or home / ".codex"
    )
    skills_dir = normalized_path(args.skills_dir or home / ".agents" / "skills")
    bin_dir = normalized_path(args.bin_dir or home / ".local" / "bin")
    for label, path in (
        ("Codex home", codex_home),
        ("skills directory", skills_dir),
        ("binary directory", bin_dir),
    ):
        if path == Path(path.anchor):
            raise InstallError(f"refusing to use filesystem root as {label}")
    return codex_home, skills_dir, bin_dir


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Install token-efficient, evidence-preserving research tooling for the current user."
        )
    )
    parser.add_argument("--codex-home", type=Path, help="default: CODEX_HOME or ~/.codex")
    parser.add_argument(
        "--skills-dir", type=Path, help="default: ~/.agents/skills (official user skill scope)"
    )
    parser.add_argument("--bin-dir", type=Path, help="default: ~/.local/bin")
    parser.add_argument("--dry-run", action="store_true", help="show changes without writing")
    parser.add_argument("--json", action="store_true", help="emit compact JSON")
    return parser


def print_human(result: dict[str, object]) -> None:
    changed = result["changed"]
    assert isinstance(changed, list)
    print(f"{result['status']}: {len(changed)} target(s) would change" if result["status"] == "dry-run" else f"{result['status']}: {len(changed)} target(s) changed")
    for item in changed:
        assert isinstance(item, dict)
        print(f"  {item['action']}: {item['path']}")
    if result.get("backup_dir"):
        print(f"  backups: {result['backup_dir']}")
    for warning in result.get("warnings", []):
        print(f"warning: {warning}", file=sys.stderr)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    environ = dict(os.environ)
    try:
        codex_home, skills_dir, bin_dir = default_locations(
            args, environ=environ, home=Path.home()
        )
        repo_root = Path(__file__).resolve().parents[1]
        targets = build_targets(
            repo_root,
            codex_home=codex_home,
            skills_dir=skills_dir,
            bin_dir=bin_dir,
        )
        result = install_targets(targets, codex_home=codex_home, dry_run=args.dry_run)
        result["warnings"] = path_warnings(
            codex_home=codex_home,
            skills_dir=skills_dir,
            bin_dir=bin_dir,
            environ=environ,
        )
        result["restart_required"] = bool(result["changed"]) and not args.dry_run
    except InstallError as exc:
        if args.json:
            print(json.dumps({"schema": 1, "status": "error", "error": str(exc)}))
        else:
            print(f"install_codex_research_loop: {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    else:
        print_human(result)
        if result["restart_required"]:
            print("Start a new Codex session to load the global skill, policy, and monitor.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
