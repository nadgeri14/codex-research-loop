#!/usr/bin/env python3
"""Gated Muse CLI lane — worktree-isolated ``muse exec`` for tasks the
one-shot direct-API worker cannot complete.

Escalation-only second lane. The default implementation path remains
``scripts/muse_research_worker.py`` (direct API, no tools). Use this lane only
when a contract needs repository exploration or iterative test-fixing and has
bounced off the direct worker (e.g. correction ceiling).

Design:

- The agent never runs in the main repository. The wrapper creates a detached
  git worktree under the canonical scratch root and runs ``muse exec`` there.
- ``--yolo`` + ``--user-input-auto-resolve`` + ``stdin=/dev/null`` remove every
  interactive stall source (approval prompts, sandbox proxy, user-input
  requests). Isolation comes from the scratch worktree and the
  review-before-apply gate, not from the sandbox.
- Hard wall deadline enforced by process-group TERM->KILL. Bounded stdout/
  stderr capture to private evidence (0700/0600) under scratch.
- After the run: main-repo contamination check (fail closed), diff extraction
  (``git add -A`` + ``git diff --cached --binary --no-renames``), owned-path
  scope check, caller-declared ``--check-json`` validations inside the
  worktree.
- Nothing is applied automatically. The run produces ``patch.diff`` +
  ``patch.meta.json`` in evidence for Sol-high review; a favorable review is
  followed by a separate ``--apply-patch`` invocation that re-verifies scope
  and applies to the main repository.
- Attempt ceilings, unchanged-failure suppression, and the repo lock are
  shared-in-spirit with the direct worker (same repo lock file, so the two
  lanes exclude each other on one repository).

Model identity is fixed: ``muse-spark-1.2-contributor`` at ``xhigh``.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

SCHEMA_VERSION = 1
MODEL_ID = "muse-spark-1.2-contributor"
EFFORT = "xhigh"

DEFAULT_SCRATCH_ROOT = "/lustre/nvwulf/scratch/anadgeri/codex-cache"


def canonical_scratch_root() -> str:
    """Machine-local canonical scratch anchor.

    Overridable via MUSE_WORKER_SCRATCH_ROOT so the package is genuinely
    portable to other machines/accounts; every scratch path (and the shared
    repo lock) is anchored beneath this root.
    """
    return os.environ.get("MUSE_WORKER_SCRATCH_ROOT", DEFAULT_SCRATCH_ROOT)

DEF_WALL_SECONDS = 1800
HARD_WALL = 7200
DEF_MAX_MODEL_STEPS = 40
HARD_MAX_MODEL_STEPS = 200
DEF_MAX_TOOL_OUTPUT_BYTES = 131072
HARD_MAX_TOOL_OUTPUT_BYTES = 4 * 1024 * 1024
DEF_GRACE_SECONDS = 5
DEF_MAX_INITIAL = 3
DEF_MAX_CORRECTIONS = 3
MAX_CONTRACT_BYTES = 200 * 1024
MAX_PATCH_BYTES = 5 * 1024 * 1024
MAX_MUSE_STDOUT = 16 * 1024 * 1024
MAX_MUSE_STDERR = 2 * 1024 * 1024
MAX_VALIDATION_STDOUT = 1 * 1024 * 1024
MAX_VALIDATION_STDERR = 1 * 1024 * 1024
MAX_CHECKS = 32
GIT_TIMEOUT = 60

EXIT_SUCCESS = 0
EXIT_PREFLIGHT = 10
EXIT_LOCK = 12
EXIT_SUPPRESSED = 13
EXIT_CEILING = 14
EXIT_VALIDATION_FAILED = 22
EXIT_VALIDATION_TIMEOUT = 23
EXIT_INTERRUPTED = 24
EXIT_PERSISTENCE = 25
EXIT_MUSE_MISSING = 30
EXIT_MUSE_FAILED = 31
EXIT_MUSE_TIMEOUT = 32
EXIT_NO_CHANGES = 33
EXIT_SCOPE = 34
EXIT_CONTAMINATION = 35
EXIT_PATCH_OVERSIZED = 36
EXIT_APPLY_INVALID = 40
EXIT_APPLY_SCOPE = 41
EXIT_APPLY_FAILED = 42

# Transient outcomes: recorded, but they neither trigger unchanged-failure
# suppression nor count toward attempt ceilings — an interrupt or a one-off
# deadline overrun must not permanently block the identical retry.
TRANSIENT_EXITS = frozenset({EXIT_INTERRUPTED, EXIT_MUSE_TIMEOUT, EXIT_VALIDATION_TIMEOUT})

CLASS_FOR = {
    EXIT_SUCCESS: "success",
    EXIT_PREFLIGHT: "preflight",
    EXIT_LOCK: "lock_contention",
    EXIT_SUPPRESSED: "unchanged_failure_suppressed",
    EXIT_CEILING: "attempt_ceiling",
    EXIT_VALIDATION_FAILED: "validation_failed",
    EXIT_VALIDATION_TIMEOUT: "validation_timeout",
    EXIT_INTERRUPTED: "interrupted",
    EXIT_PERSISTENCE: "persistence_failure",
    EXIT_MUSE_MISSING: "muse_binary_missing",
    EXIT_MUSE_FAILED: "muse_exec_failed",
    EXIT_MUSE_TIMEOUT: "muse_exec_timeout",
    EXIT_NO_CHANGES: "no_changes_produced",
    EXIT_SCOPE: "scope_violation",
    EXIT_CONTAMINATION: "main_repo_contamination",
    EXIT_PATCH_OVERSIZED: "patch_oversized",
    EXIT_APPLY_INVALID: "apply_patch_invalid",
    EXIT_APPLY_SCOPE: "apply_scope_violation",
    EXIT_APPLY_FAILED: "apply_failed",
}


def eprint(*a, **kw):
    print(*a, file=sys.stderr, **kw)


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def sanitize(cid: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "_", cid)[:64] or "default"


def is_tmpfs(path: str) -> bool:
    canon = os.path.realpath(os.path.abspath(path))
    with open("/proc/mounts", "r", encoding="utf-8") as f:
        lines = f.readlines()
    best_len = -1
    best_fs = None
    for line in lines:
        parts = line.split()
        if len(parts) < 3:
            continue
        mnt = parts[1].replace("\\040", " ")
        fs = parts[2]
        if canon == mnt or canon.startswith(mnt.rstrip("/") + "/"):
            if len(mnt) > best_len:
                best_len = len(mnt)
                best_fs = fs
    return best_fs in ("tmpfs", "ramfs")


def validate_scratch(scratch_root: str) -> str:
    if not scratch_root:
        raise ValueError("scratch_root empty")
    if os.path.islink(os.path.abspath(scratch_root)):
        raise ValueError(f"scratch {scratch_root} is symlink")
    canon = os.path.realpath(os.path.abspath(scratch_root))
    if canon in ("/tmp", "/dev/shm") or canon.startswith("/tmp/") or canon.startswith("/dev/shm/"):
        raise ValueError(f"scratch {canon} forbidden /tmp or /dev/shm")
    allowed = os.path.realpath(canonical_scratch_root())
    if allowed in ("/", "/tmp", "/dev/shm") or allowed.startswith("/tmp/") or allowed.startswith("/dev/shm/"):
        raise ValueError(f"canonical scratch root {allowed} forbidden")
    if not (canon == allowed or canon.startswith(allowed.rstrip("/") + "/")):
        raise ValueError(f"scratch {canon} not beneath {allowed}")
    if is_tmpfs(canon):
        raise ValueError(f"scratch {canon} is tmpfs/ramfs")
    if not os.path.isdir(canon):
        os.makedirs(canon, mode=0o700, exist_ok=True)
    probe = os.path.join(canon, ".probe_" + uuid.uuid4().hex[:8])
    fd = os.open(probe, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.write(fd, b"ok")
    finally:
        os.close(fd)
    os.unlink(probe)
    return canon


def owned_match(path: str, owned: List[str]) -> bool:
    for o in owned:
        on = o.rstrip("/")
        if path == on or path.startswith(on + "/"):
            return True
    return False


def run_git(cwd: str, args: List[str], timeout: int = GIT_TIMEOUT) -> subprocess.CompletedProcess:
    return subprocess.run(["git"] + args, cwd=cwd, capture_output=True, timeout=timeout)


SNAPSHOT_GIT_TIMEOUT = 300


def repo_snapshot(root: str) -> Dict[str, str]:
    """Compact identity of the main repository used to detect contamination."""
    try:
        r = run_git(root, ["rev-parse", "HEAD"])
        if r.returncode != 0:
            raise ValueError(f"rev-parse HEAD failed: {r.stderr.decode(errors='ignore')[:200]}")
        head = r.stdout.decode().strip()
        r = run_git(root, ["status", "--porcelain=v1", "-z", "--untracked-files=all"], timeout=SNAPSHOT_GIT_TIMEOUT)
        if r.returncode != 0:
            raise ValueError(f"status failed: {r.stderr.decode(errors='ignore')[:200]}")
        status_hash = sha256_bytes(r.stdout)
        d1 = run_git(root, ["diff", "--binary"], timeout=SNAPSHOT_GIT_TIMEOUT).stdout
        d2 = run_git(root, ["diff", "--cached", "--binary"], timeout=SNAPSHOT_GIT_TIMEOUT).stdout
    except subprocess.TimeoutExpired as exc:
        raise ValueError(f"repo snapshot timed out: {exc}")
    return {"head": head, "status_hash": status_hash, "diff_hash": sha256_bytes(d1 + d2)}


def validate_check_argv(argv: List[str], owned: List[str]) -> Optional[str]:
    if not argv or not all(isinstance(a, str) and a for a in argv):
        return "argv empty/invalid"
    shell_bins = {"bash", "sh", "zsh", "dash", "fish", "ksh"}
    base0 = os.path.basename(argv[0])
    if base0 in shell_bins and any(a == "-c" for a in argv[1:]):
        return "shell -c rejected"
    for a in argv:
        if "\0" in a:
            return "NUL in argv"
        s = a.strip()
        if s.startswith("bash -c") or s.startswith("sh -c"):
            return "shell string rejected"
    low = " ".join(argv).lower()
    if ".git" in low and any(cmd in low for cmd in ("rm ", "rm\t", "mv ", "unlink", "rmtree")):
        return ".git mutation rejected"
    for arg in argv[1:]:
        if arg.startswith("-"):
            continue
        if "/" in arg and ".." in arg.split("/"):
            return f"path traversal in argv {arg!r}"
    if base0 in ("rm", "mv", "unlink", "cp", "chmod", "chown"):
        for arg in argv[1:]:
            if arg.startswith("-"):
                continue
            if not owned_match(arg, owned):
                return f"validation alters outside-owned {arg!r} rejected"
    return None


def _state_base(scratch: str, repo_canonical: str, contract_id: str) -> Path:
    repo_hash = hashlib.sha256(repo_canonical.encode()).hexdigest()[:16]
    return Path(scratch) / "muse-cli-worker-state" / f"{repo_hash}_{sanitize(contract_id)}"


def load_state(scratch: str, repo_canonical: str, contract_id: str) -> Dict[str, Any]:
    p = _state_base(scratch, repo_canonical, contract_id) / "state.json"
    if not p.is_file():
        return {"attempts": []}
    if os.path.islink(str(p)):
        raise ValueError("state is symlink")
    st = os.lstat(str(p))
    if not stat.S_ISREG(st.st_mode):
        raise ValueError("state not regular")
    if st.st_mode & 0o077:
        raise ValueError(f"state unsafe perms {oct(st.st_mode & 0o777)}")
    if st.st_size > 1024 * 1024:
        raise ValueError("state oversized")
    fd = os.open(str(p), os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        data_bytes = b""
        while True:
            chunk = os.read(fd, 8192)
            if not chunk:
                break
            data_bytes += chunk
            if len(data_bytes) > 1024 * 1024:
                raise ValueError("state oversized")
    finally:
        os.close(fd)
    data = json.loads(data_bytes.decode("utf-8"))
    if not isinstance(data, dict) or not isinstance(data.get("attempts"), list):
        raise ValueError("malformed state")
    return data


def save_state_atomic(scratch: str, repo_canonical: str, contract_id: str, state: Dict[str, Any]) -> None:
    base = _state_base(scratch, repo_canonical, contract_id)
    base.mkdir(parents=True, exist_ok=True)
    os.chmod(base, 0o700)
    tmp = base / f".state.{uuid.uuid4().hex}.tmp"
    fd = os.open(str(tmp), os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        os.write(fd, json.dumps(state, indent=2, sort_keys=True).encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    tmp.replace(base / "state.json")
    try:
        dfd = os.open(str(base), os.O_DIRECTORY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    except Exception:
        pass


def _pgroup_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except Exception:
        return True


def kill_pgroup(pgid: int, proc: subprocess.Popen, grace: int = DEF_GRACE_SECONDS) -> bool:
    """TERM then KILL the whole process group; return True if an orphan survives.

    Escalation is gated on the GROUP still existing, not on the direct child:
    a TERM-ignoring grandchild must be SIGKILLed even after the leader exits.
    """
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        return False
    except Exception:
        pass
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        if proc.poll() is not None and not _pgroup_alive(pgid):
            break
        time.sleep(0.05)
    if _pgroup_alive(pgid):
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except Exception:
            pass
    try:
        proc.wait(timeout=2)
    except Exception:
        pass
    time.sleep(0.1)
    return _pgroup_alive(pgid)


def run_captured(argv: List[str], cwd: str, env: Dict[str, str], wall_deadline: float,
                 out_path: Path, err_path: Path, out_cap: int, err_cap: int,
                 interrupted: Dict[str, bool]) -> Dict[str, Any]:
    """Run argv in its own process group with bounded capture and a hard deadline.

    Direct-to-file capture via reader threads: no pipe-buffer deadlock, and the
    child can never block the wrapper past the wall deadline.
    """
    for p in (out_path, err_path):
        fd = os.open(str(p), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
        os.close(fd)
    proc = subprocess.Popen(argv, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            start_new_session=True)
    pgid = proc.pid
    counts = {"out": 0, "err": 0}
    trunc = {"out": False, "err": False}

    def drain(pipe, path: Path, key: str, cap: int):
        try:
            with open(path, "ab") as fh:
                while True:
                    chunk = pipe.read(65536)
                    if not chunk:
                        break
                    if counts[key] < cap:
                        allowed = min(len(chunk), cap - counts[key])
                        fh.write(chunk[:allowed])
                        fh.flush()
                        counts[key] += allowed
                        if allowed < len(chunk):
                            trunc[key] = True
                    else:
                        trunc[key] = True
        except Exception:
            pass

    t_out = threading.Thread(target=drain, args=(proc.stdout, out_path, "out", out_cap), daemon=True)
    t_err = threading.Thread(target=drain, args=(proc.stderr, err_path, "err", err_cap), daemon=True)
    t_out.start()
    t_err.start()
    timed_out = False
    while True:
        if proc.poll() is not None:
            break
        if interrupted.get("flag") or time.monotonic() > wall_deadline:
            timed_out = True
            break
        try:
            proc.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            pass
    orphan = False
    if timed_out:
        orphan = kill_pgroup(pgid, proc)
    t_out.join(timeout=5)
    t_err.join(timeout=5)
    if not timed_out:
        try:
            os.killpg(pgid, 0)
            orphan = kill_pgroup(pgid, proc)
        except ProcessLookupError:
            pass
        except Exception:
            pass
    return {
        "exit_code": proc.returncode if not timed_out else None,
        "timed_out": timed_out,
        "interrupted": bool(interrupted.get("flag")),
        "orphan": orphan,
        "stdout_bytes": counts["out"],
        "stderr_bytes": counts["err"],
        "stdout_truncated": trunc["out"],
        "stderr_truncated": trunc["err"],
    }


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Gated Muse CLI lane — worktree-isolated muse exec")
    p.add_argument("--root", required=True, help="Main repository root")
    p.add_argument("--contract-id", required=True)
    p.add_argument("--contract", help="Contract file path (run mode)")
    p.add_argument("--attempt-kind", choices=["initial", "correction"], default="initial")
    p.add_argument("--owned-path", action="append", dest="owned_paths", default=[],
                   help="Owned relative path (repeatable)")
    p.add_argument("--check-json", action="append", dest="checks", default=[],
                   help="JSON array argv run in the worktree after muse finishes (repeatable)")
    p.add_argument("--scratch-root", default=DEFAULT_SCRATCH_ROOT)
    p.add_argument("--wall-seconds", type=int, default=DEF_WALL_SECONDS)
    p.add_argument("--max-model-steps", type=int, default=DEF_MAX_MODEL_STEPS)
    p.add_argument("--max-tool-output-bytes", type=int, default=DEF_MAX_TOOL_OUTPUT_BYTES)
    p.add_argument("--allow-empty", action="store_true",
                   help="Authorize a run that produces no changes (default fail-closed)")
    p.add_argument("--muse-bin", default="muse", help="muse binary (tests may point at a fake)")
    p.add_argument("--apply-patch", help="Apply mode: patch file from a reviewed prior run")
    p.add_argument("--allow-base-mismatch", action="store_true",
                   help="Apply mode: proceed although HEAD differs from the reviewed patch's base (or the patch has no meta sidecar); default fail-closed")
    args = p.parse_args(argv)
    if not args.owned_paths:
        p.error("at least one --owned-path required")
    if args.apply_patch is None and not args.contract:
        p.error("--contract required in run mode")
    if args.wall_seconds <= 0 or args.wall_seconds > HARD_WALL:
        p.error(f"--wall-seconds must be 1..{HARD_WALL}")
    if args.max_model_steps <= 0 or args.max_model_steps > HARD_MAX_MODEL_STEPS:
        p.error(f"--max-model-steps must be 1..{HARD_MAX_MODEL_STEPS}")
    if args.max_tool_output_bytes <= 0 or args.max_tool_output_bytes > HARD_MAX_TOOL_OUTPUT_BYTES:
        p.error(f"--max-tool-output-bytes must be 1..{HARD_MAX_TOOL_OUTPUT_BYTES}")
    for o in args.owned_paths:
        if "\0" in o or os.path.isabs(o) or ".." in o.split("/"):
            p.error(f"--owned-path invalid {o!r}")
    parsed_checks: List[List[str]] = []
    for c in args.checks:
        try:
            arr = json.loads(c)
        except Exception as exc:
            p.error(f"--check-json invalid JSON: {exc}")
        if not isinstance(arr, list) or not arr or not all(isinstance(x, str) and x for x in arr):
            p.error("--check-json must be JSON array of nonempty strings")
        parsed_checks.append(arr)
    if len(parsed_checks) > MAX_CHECKS:
        p.error(f"too many checks > {MAX_CHECKS}")
    args.parsed_checks = parsed_checks
    return args


def build_prompt(contract_text: str, owned: List[str], checks: List[List[str]]) -> str:
    lines = [
        "# ROLE",
        "You are Muse Spark, the sole implementation worker for this research",
        "loop, running inside an ISOLATED, DISPOSABLE git worktree. Your diff is",
        "extracted after the run and applied to the real repository only after",
        "an independent review.",
        "",
        "# BOUNDARIES (mandatory)",
        "- Modify ONLY these owned paths (relative to the worktree root):",
    ]
    for o in owned:
        lines.append(f"  - {o}")
    lines += [
        "- Never write outside the worktree. Never touch credentials, `.git`,",
        "  user-wide configuration, or unrelated files.",
        "- Do not commit; leave changes in the working tree.",
        "- The caller will run these validation commands after you finish;",
        "  make them pass:",
    ]
    for c in checks:
        lines.append(f"  - {json.dumps(c)}")
    if not checks:
        lines.append("  - (none declared; run the checks the contract names)")
    lines += [
        "- If any requirement needs a scientific or experimental decision, stop",
        "  and report the exact ambiguity in your final message. Do not choose",
        "  research behavior.",
        "",
        "# IMPLEMENTATION CONTRACT",
        contract_text,
    ]
    return "\n".join(lines)


def numstat_paths(root: str, patch_path: str) -> Optional[List[str]]:
    r = run_git(root, ["apply", "--numstat", "-z", "--binary", patch_path])
    if r.returncode != 0:
        return None
    paths: List[str] = []
    for ent in r.stdout.split(b"\0"):
        if not ent:
            continue
        txt = ent.decode("utf-8", errors="surrogateescape")
        parts = txt.split("\t")
        if len(parts) >= 3:
            # the path itself may contain literal tabs (unquoted in -z mode):
            # rejoin everything after the two count fields
            paths.append("\t".join(parts[2:]))
        elif len(parts) == 1 and txt:
            # -z rename continuation record (patches are generated with
            # --no-renames, but stay fail-closed if one appears anyway)
            paths.append(txt)
    return paths


def main(argv=None) -> int:
    os.umask(0o077)
    start_wall = time.monotonic()
    invocation = uuid.uuid4().hex
    try:
        args = parse_args(argv)
    except SystemExit as e:
        return EXIT_PREFLIGHT if e.code != 0 else 0
    try:
        scratch = validate_scratch(args.scratch_root)
    except Exception as exc:
        eprint(f"preflight scratch: {exc}")
        return EXIT_PREFLIGHT
    try:
        ev_base = Path(scratch) / "muse-cli-evidence" / sanitize(args.contract_id) / invocation
        ev_base.mkdir(parents=True, mode=0o700, exist_ok=True)
        os.chmod(ev_base, 0o700)
    except Exception as exc:
        eprint(f"preflight evidence: {exc}")
        return EXIT_PREFLIGHT

    def wtext(name: str, t: str) -> Path:
        p = ev_base / name
        p.write_text(t, encoding="utf-8")
        os.chmod(p, 0o600)
        return p

    def wbytes(name: str, b: bytes) -> Path:
        p = ev_base / name
        p.write_bytes(b)
        os.chmod(p, 0o600)
        return p

    def wjson(name: str, obj: Any) -> Path:
        p = ev_base / name
        p.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")
        os.chmod(p, 0o600)
        return p

    def finish(code: int, extra: Optional[Dict[str, Any]] = None) -> int:
        summary = {"schema_version": SCHEMA_VERSION, "invocation": invocation,
                   "contract_id": args.contract_id, "classification": CLASS_FOR[code],
                   "exit_code": code, "evidence_dir": str(ev_base),
                   "wall_seconds": round(time.monotonic() - start_wall, 2)}
        if extra:
            summary.update(extra)
        try:
            wjson("summary.json", summary)
        except Exception:
            pass
        eprint(f"evidence: {ev_base}")
        return code

    interrupted: Dict[str, bool] = {"flag": False}
    orig_sigint = signal.getsignal(signal.SIGINT)
    orig_sigterm = signal.getsignal(signal.SIGTERM)

    def sig_handler(signum, frame):
        interrupted["flag"] = True

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)
    wall_deadline = start_wall + args.wall_seconds
    import fcntl
    repo_lock_fd = None
    wt_path: Optional[Path] = None
    try:
        try:
            root = os.path.realpath(os.path.abspath(args.root))
            if not os.path.isdir(root):
                raise ValueError(f"root not directory {root}")
            r = run_git(root, ["rev-parse", "--is-inside-work-tree"])
            if r.returncode != 0 or r.stdout.decode().strip() != "true":
                raise ValueError("root is not a git work tree")
        except Exception as exc:
            eprint(f"preflight root: {exc}")
            return finish(EXIT_PREFLIGHT, {"error": str(exc)[:1000]})
        for check in args.parsed_checks:
            err = validate_check_argv(check, args.owned_paths)
            if err:
                eprint(f"preflight check rejected: {err}")
                return finish(EXIT_PREFLIGHT, {"error": err})
        # Shared repo lock file namespace with the direct worker: the two lanes
        # exclude each other on one repository. Anchored at the CANONICAL
        # scratch root (not the caller-narrowed --scratch-root) so the mutual
        # exclusion holds regardless of which scratch subtree each run uses.
        try:
            lock_dir = Path(os.path.realpath(canonical_scratch_root())) / "muse-worker-locks"
            lock_dir.mkdir(parents=True, exist_ok=True)
            os.chmod(lock_dir, 0o700)
            repo_hash = hashlib.sha256(root.encode()).hexdigest()[:16]
            fd = os.open(str(lock_dir / f"repo-{repo_hash}.lock"), os.O_CREAT | os.O_RDWR, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                os.close(fd)
                eprint("lock contention repo")
                return finish(EXIT_LOCK)
            repo_lock_fd = fd
        except Exception as exc:
            eprint(f"lock error: {exc}")
            return finish(EXIT_PREFLIGHT, {"error": str(exc)[:1000]})

        # ------------------------------------------------------------------
        # Apply mode: re-verify scope and apply a reviewed patch.
        # ------------------------------------------------------------------
        if args.apply_patch:
            patch_path = os.path.realpath(os.path.abspath(args.apply_patch))
            if not os.path.isfile(patch_path) or os.path.islink(os.path.abspath(args.apply_patch)):
                eprint(f"apply: patch not a regular file {patch_path}")
                return finish(EXIT_APPLY_INVALID)
            patch_bytes = Path(patch_path).read_bytes()
            if not patch_bytes or len(patch_bytes) > MAX_PATCH_BYTES:
                eprint("apply: patch empty or oversized")
                return finish(EXIT_APPLY_INVALID)
            wbytes("apply_patch.copy", patch_bytes)
            wtext("apply_patch.sha256", sha256_bytes(patch_bytes) + "\n")
            paths = numstat_paths(root, patch_path)
            if paths is None:
                eprint("apply: git apply --numstat failed (invalid patch)")
                return finish(EXIT_APPLY_INVALID)
            outside = sorted(p for p in set(paths) if not owned_match(p, args.owned_paths))
            if outside:
                eprint(f"apply: patch touches outside-owned paths {outside}")
                return finish(EXIT_APPLY_SCOPE, {"outside": outside})
            # Base-HEAD verification is fail-closed: a patch was reviewed
            # against one specific commit and must not silently land on
            # drifted code (or without provenance) unless explicitly allowed.
            meta_path = Path(patch_path).with_suffix(".meta.json")
            cur_head = run_git(root, ["rev-parse", "HEAD"]).stdout.decode().strip()
            base_head = None
            if meta_path.is_file():
                try:
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                    base_head = meta.get("base_head")
                except Exception:
                    base_head = None
            if base_head != cur_head and not args.allow_base_mismatch:
                if base_head is None:
                    eprint("apply: no patch.meta.json base_head (pass --allow-base-mismatch to override)")
                else:
                    eprint(f"apply: base HEAD {base_head[:12]} != current {cur_head[:12]} (pass --allow-base-mismatch to override)")
                return finish(EXIT_APPLY_INVALID, {"base_head": base_head, "current_head": cur_head, "error": "base mismatch"})
            r = run_git(root, ["apply", "--check", "--binary", patch_path])
            if r.returncode != 0:
                eprint(f"apply: git apply --check failed: {r.stderr.decode(errors='ignore')[:500]}")
                return finish(EXIT_APPLY_FAILED, {"error": r.stderr.decode(errors="ignore")[:1000]})
            r = run_git(root, ["apply", "--binary", patch_path])
            if r.returncode != 0:
                eprint(f"apply: git apply failed: {r.stderr.decode(errors='ignore')[:500]}")
                return finish(EXIT_APPLY_FAILED, {"error": r.stderr.decode(errors="ignore")[:1000]})

            def rollback() -> bool:
                rr = run_git(root, ["apply", "-R", "--binary", patch_path])
                return rr.returncode == 0

            # Caller-declared checks gate the apply: run them in the MAIN
            # repository; any failure rolls the patch back.
            val_results = []
            for idx, check in enumerate(args.parsed_checks):
                vres = run_captured(check, root, os.environ.copy(), wall_deadline,
                                    ev_base / f"apply_validation_{idx}_stdout.log",
                                    ev_base / f"apply_validation_{idx}_stderr.log",
                                    MAX_VALIDATION_STDOUT, MAX_VALIDATION_STDERR, interrupted)
                val_results.append({"argv": check, **vres})
                if vres["interrupted"] or vres["timed_out"] or vres["exit_code"] != 0:
                    wjson("apply_validation_results.json", val_results)
                    rolled = rollback()
                    if vres["interrupted"]:
                        code = EXIT_INTERRUPTED
                    elif vres["timed_out"]:
                        code = EXIT_VALIDATION_TIMEOUT
                    else:
                        code = EXIT_VALIDATION_FAILED
                    if not rolled:
                        eprint("apply: post-apply check failed AND rollback failed — repository needs manual attention")
                        return finish(EXIT_PERSISTENCE, {"error": "check failed and rollback failed", "failed_idx": idx})
                    eprint(f"apply: post-apply check {idx} failed; patch rolled back")
                    return finish(code, {"failed_idx": idx, "rolled_back": True})
            if val_results:
                wjson("apply_validation_results.json", val_results)
            print(f"cli-lane apply: success paths={sorted(set(paths))} checks={len(val_results)}")
            return finish(EXIT_SUCCESS, {"mode": "apply", "applied_paths": sorted(set(paths)), "validations": len(val_results)})

        # ------------------------------------------------------------------
        # Run mode.
        # ------------------------------------------------------------------
        contract_path = os.path.realpath(os.path.abspath(args.contract))
        if not os.path.isfile(contract_path) or os.path.islink(os.path.abspath(args.contract)):
            eprint(f"preflight contract: not a regular file {contract_path}")
            return finish(EXIT_PREFLIGHT)
        contract_bytes = Path(contract_path).read_bytes()
        if not contract_bytes or len(contract_bytes) > MAX_CONTRACT_BYTES:
            eprint("preflight contract: empty or oversized")
            return finish(EXIT_PREFLIGHT)
        contract_hash = sha256_bytes(contract_bytes)
        wbytes("contract.copy", contract_bytes)
        muse_bin = shutil.which(args.muse_bin) if os.sep not in args.muse_bin else (
            args.muse_bin if os.path.isfile(args.muse_bin) and os.access(args.muse_bin, os.X_OK) else None)
        if not muse_bin:
            eprint(f"muse binary not found/executable: {args.muse_bin}")
            return finish(EXIT_MUSE_MISSING)
        try:
            state = load_state(scratch, root, args.contract_id)
        except Exception as exc:
            eprint(f"state malformed: {exc}")
            return finish(EXIT_PERSISTENCE, {"error": str(exc)[:1000]})
        attempts = state.get("attempts", [])
        # transient outcomes (interrupt, deadline overrun) do not consume ceilings
        counted = [a for a in attempts if a.get("exit_code") not in TRANSIENT_EXITS]
        initial_cnt = sum(1 for a in counted if a.get("attempt_kind") == "initial")
        corr_cnt = sum(1 for a in counted if a.get("attempt_kind") == "correction")
        if args.attempt_kind == "initial" and initial_cnt >= DEF_MAX_INITIAL:
            eprint(f"initial ceiling {initial_cnt} >= {DEF_MAX_INITIAL}")
            return finish(EXIT_CEILING)
        if args.attempt_kind == "correction" and corr_cnt >= DEF_MAX_CORRECTIONS:
            eprint(f"correction ceiling {corr_cnt} >= {DEF_MAX_CORRECTIONS}")
            return finish(EXIT_CEILING)
        fp = sha256_bytes(json.dumps({
            "contract_hash": contract_hash, "owned": args.owned_paths,
            "checks": args.parsed_checks, "model": MODEL_ID,
            "wall": args.wall_seconds, "steps": args.max_model_steps,
        }, sort_keys=True).encode())
        for a in reversed(attempts):
            if (a.get("fingerprint") == fp and a.get("exit_code") != EXIT_SUCCESS
                    and a.get("exit_code") not in TRANSIENT_EXITS):
                eprint(f"suppressed unchanged fingerprint {fp[:8]}")
                return finish(EXIT_SUPPRESSED, {"fingerprint": fp})

        def record_attempt(code: int) -> None:
            state["attempts"].append({
                "invocation": invocation, "fingerprint": fp,
                "attempt_kind": args.attempt_kind,
                "classification": CLASS_FOR[code], "exit_code": code,
                "timestamp": time.time(), "lane": "cli",
            })
            try:
                save_state_atomic(scratch, root, args.contract_id, state)
            except Exception as exc:
                eprint(f"state save failed: {exc}")

        try:
            before = repo_snapshot(root)
        except Exception as exc:
            eprint(f"baseline snapshot failed: {exc}")
            return finish(EXIT_PREFLIGHT, {"error": str(exc)[:1000]})
        wjson("before_repo.json", before)
        dirty = run_git(root, ["status", "--porcelain=v1"]).stdout.decode(errors="ignore")
        if dirty.strip():
            eprint("note: main repo is dirty; the worktree sees committed HEAD only")
            wtext("main_repo_dirty.txt", dirty[:20000])

        wt_root = Path(scratch) / "muse-cli-worktrees" / sanitize(args.contract_id)
        wt_root.mkdir(parents=True, exist_ok=True)
        os.chmod(wt_root, 0o700)
        wt_path = wt_root / invocation
        r = run_git(root, ["worktree", "add", "--detach", str(wt_path), "HEAD"])
        if r.returncode != 0:
            eprint(f"worktree add failed: {r.stderr.decode(errors='ignore')[:500]}")
            wt_path = None
            return finish(EXIT_PREFLIGHT, {"error": "worktree add failed"})

        prompt = build_prompt(contract_bytes.decode("utf-8", errors="replace"),
                              args.owned_paths, args.parsed_checks)
        prompt_path = wtext("prompt.txt", prompt)
        scratch_env = ev_base / "scratch_env"
        scratch_env.mkdir(mode=0o700, exist_ok=True)
        for name in ("tmp", "xdg"):
            (scratch_env / name).mkdir(mode=0o700, exist_ok=True)
        env = os.environ.copy()
        env["TMPDIR"] = str(scratch_env / "tmp")
        env["TMP"] = str(scratch_env / "tmp")
        env["TEMP"] = str(scratch_env / "tmp")
        env["XDG_CACHE_HOME"] = str(scratch_env / "xdg")

        muse_argv = [
            muse_bin, "exec",
            "--json",
            "--yolo",
            "--user-input-auto-resolve",
            "--no-foreign-personal-context",
            "--disable-web-tools",
            "--model", MODEL_ID,
            "--reasoning-effort", EFFORT,
            "--max-model-steps", str(args.max_model_steps),
            "--max-tool-output-bytes", str(args.max_tool_output_bytes),
            "--prompt-file", str(prompt_path),
        ]
        wjson("muse_argv.json", muse_argv)
        res = run_captured(muse_argv, str(wt_path), env, wall_deadline,
                           ev_base / "muse_stdout.jsonl", ev_base / "muse_stderr.log",
                           MAX_MUSE_STDOUT, MAX_MUSE_STDERR, interrupted)
        wjson("muse_result.json", res)
        # Main-repo integrity is verified on EVERY post-run path — the runs
        # most likely to have misbehaved (timeout, crash, interrupt) must not
        # skip the contamination check.
        try:
            after = repo_snapshot(root)
        except Exception as exc:
            eprint(f"cannot verify main repo integrity after run: {exc}")
            record_attempt(EXIT_PERSISTENCE)
            code = finish(EXIT_PERSISTENCE, {"error": str(exc)[:1000], "muse_result": res, "worktree": str(wt_path)})
            wt_path = None
            return code
        wjson("after_repo.json", after)
        if after != before:
            eprint("main repository changed during the run — contamination, fail closed")
            record_attempt(EXIT_CONTAMINATION)
            code = finish(EXIT_CONTAMINATION, {"before": before, "after": after, "muse_result": res, "worktree": str(wt_path)})
            wt_path = None
            return code
        if res["orphan"]:
            eprint("process-group orphan survived kill escalation — fail closed")
            record_attempt(EXIT_PERSISTENCE)
            code = finish(EXIT_PERSISTENCE, {"error": "orphan process survived", "muse_result": res, "worktree": str(wt_path)})
            wt_path = None
            return code
        if res["interrupted"]:
            record_attempt(EXIT_INTERRUPTED)
            return finish(EXIT_INTERRUPTED)
        if res["timed_out"]:
            eprint(f"muse exec hit wall deadline ({args.wall_seconds}s); worktree kept: {wt_path}")
            record_attempt(EXIT_MUSE_TIMEOUT)
            code = finish(EXIT_MUSE_TIMEOUT, {"worktree": str(wt_path)})
            wt_path = None  # keep for inspection
            return code
        if res["exit_code"] != 0:
            eprint(f"muse exec failed exit={res['exit_code']}; worktree kept: {wt_path}")
            record_attempt(EXIT_MUSE_FAILED)
            code = finish(EXIT_MUSE_FAILED, {"muse_exit": res["exit_code"], "worktree": str(wt_path)})
            wt_path = None
            return code

        r = run_git(str(wt_path), ["add", "-A"])
        if r.returncode != 0:
            eprint(f"worktree add -A failed: {r.stderr.decode(errors='ignore')[:300]}")
            record_attempt(EXIT_PREFLIGHT)
            return finish(EXIT_PREFLIGHT, {"error": "worktree stage failed"})
        # Diff against the BASE commit, not the worktree's current HEAD: a
        # yolo agent may commit its work despite the prompt's instruction, and
        # committed work must be captured, not discarded as an empty diff.
        base_head = before["head"]
        r = run_git(str(wt_path), ["diff", "--cached", "--name-only", "-z", base_head])
        changed = sorted(p for p in r.stdout.decode(errors="surrogateescape").split("\0") if p)
        patch = run_git(str(wt_path), ["diff", "--cached", "--binary", "--no-renames", base_head], timeout=120).stdout
        if len(patch) > MAX_PATCH_BYTES:
            eprint(f"patch oversized {len(patch)} > {MAX_PATCH_BYTES}")
            record_attempt(EXIT_PATCH_OVERSIZED)
            return finish(EXIT_PATCH_OVERSIZED, {"patch_bytes": len(patch)})
        if not changed:
            if not args.allow_empty:
                eprint("muse exec produced no changes (fail-closed without --allow-empty)")
                record_attempt(EXIT_NO_CHANGES)
                return finish(EXIT_NO_CHANGES)
        outside = sorted(p for p in changed if not owned_match(p, args.owned_paths))
        patch_path = wbytes("patch.diff", patch)
        wjson("patch.meta.json", {"schema_version": SCHEMA_VERSION, "base_head": before["head"],
                                  "contract_id": args.contract_id, "contract_hash": contract_hash,
                                  "owned": args.owned_paths, "changed": changed,
                                  "patch_sha256": sha256_bytes(patch), "lane": "cli"})
        if outside:
            eprint(f"scope violation: changes outside owned paths {outside}; patch kept for inspection, nothing applied")
            record_attempt(EXIT_SCOPE)
            return finish(EXIT_SCOPE, {"outside": outside, "changed": changed, "patch": str(patch_path)})

        val_results = []
        for idx, check in enumerate(args.parsed_checks):
            if interrupted["flag"]:
                record_attempt(EXIT_INTERRUPTED)
                return finish(EXIT_INTERRUPTED)
            if time.monotonic() > wall_deadline:
                wjson("validation_results.json", val_results)
                record_attempt(EXIT_VALIDATION_TIMEOUT)
                return finish(EXIT_VALIDATION_TIMEOUT)
            vres = run_captured(check, str(wt_path), env, wall_deadline,
                                ev_base / f"validation_{idx}_stdout.log",
                                ev_base / f"validation_{idx}_stderr.log",
                                MAX_VALIDATION_STDOUT, MAX_VALIDATION_STDERR, interrupted)
            val_results.append({"argv": check, **vres})
            if vres["orphan"]:
                wjson("validation_results.json", val_results)
                record_attempt(EXIT_PERSISTENCE)
                return finish(EXIT_PERSISTENCE, {"error": "validation orphan survived", "failed_idx": idx})
            if vres["interrupted"]:
                wjson("validation_results.json", val_results)
                record_attempt(EXIT_INTERRUPTED)
                return finish(EXIT_INTERRUPTED, {"failed_idx": idx})
            if vres["timed_out"]:
                wjson("validation_results.json", val_results)
                record_attempt(EXIT_VALIDATION_TIMEOUT)
                return finish(EXIT_VALIDATION_TIMEOUT, {"failed_idx": idx})
            if vres["exit_code"] != 0:
                wjson("validation_results.json", val_results)
                record_attempt(EXIT_VALIDATION_FAILED)
                return finish(EXIT_VALIDATION_FAILED, {"failed_idx": idx})
        wjson("validation_results.json", val_results)

        record_attempt(EXIT_SUCCESS)
        print(f"cli-lane: success changed={changed}")
        print(f"patch: {patch_path}")
        print(f"next: Sol-high review of the patch, then apply with "
              f"--apply-patch {patch_path}")
        return finish(EXIT_SUCCESS, {"changed": changed, "patch": str(patch_path),
                                     "muse_exit": 0, "validations": len(val_results)})
    finally:
        signal.signal(signal.SIGINT, orig_sigint)
        signal.signal(signal.SIGTERM, orig_sigterm)
        if wt_path is not None:
            try:
                run_git(os.path.realpath(os.path.abspath(args.root)),
                        ["worktree", "remove", "--force", str(wt_path)])
                run_git(os.path.realpath(os.path.abspath(args.root)), ["worktree", "prune"])
            except Exception:
                pass
        if repo_lock_fd is not None:
            try:
                fcntl.flock(repo_lock_fd, fcntl.LOCK_UN)
            except Exception:
                pass
            try:
                os.close(repo_lock_fd)
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())
