#!/usr/bin/env python3
"""Gated Muse CLI lane tests — fake muse binary, scratch-backed, no real model."""
import fcntl
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRATCH_BASE = Path("/lustre/nvwulf/scratch/anadgeri/codex-cache/tmp/muse-cli-worker-tests")
SCRATCH_BASE.mkdir(parents=True, exist_ok=True)
try:
    os.chmod(SCRATCH_BASE, 0o700)
except Exception:
    pass
WRAPPER = str(REPO_ROOT / "scripts" / "muse_cli_worker.py")

FAKE_MUSE_SOURCE = r'''#!/usr/bin/env python3
"""Fake muse binary for CLI-lane tests. Behavior chosen via FAKE_MUSE_MODE."""
import json, os, sys, time

def arg_after(flag):
    argv = sys.argv[1:]
    for i, a in enumerate(argv):
        if a == flag and i + 1 < len(argv):
            return argv[i + 1]
    return None

dump = os.environ.get("FAKE_MUSE_ARGV_DUMP")
if dump:
    with open(dump, "w") as fh:
        json.dump({"argv": sys.argv[1:], "cwd": os.getcwd(),
                   "prompt": open(arg_after("--prompt-file")).read() if arg_after("--prompt-file") else None}, fh)

def do_edits():
    for spec in json.loads(os.environ.get("FAKE_MUSE_EDITS", "[]")):
        p = spec["path"]
        d = os.path.dirname(p)
        if d:
            os.makedirs(d, exist_ok=True)
        with open(p, "w") as fh:
            fh.write(spec["content"])

mode = os.environ.get("FAKE_MUSE_MODE", "noop")
print(json.dumps({"event": "session.start", "mode": mode}))
if mode == "edit":
    do_edits()
elif mode == "commit":
    import subprocess
    do_edits()
    subprocess.run(["git", "add", "-A"], check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "agent commit"], check=True, capture_output=True)
elif mode == "outside":
    with open("outside_cli.txt", "w") as fh:
        fh.write("evil")
elif mode == "contaminate":
    with open(os.path.join(os.environ["FAKE_MUSE_MAIN_ROOT"], "contaminated.txt"), "w") as fh:
        fh.write("evil")
elif mode == "contaminate_fail":
    with open(os.path.join(os.environ["FAKE_MUSE_MAIN_ROOT"], "contaminated.txt"), "w") as fh:
        fh.write("evil")
    sys.exit(3)
elif mode == "sleep":
    time.sleep(float(os.environ.get("FAKE_MUSE_SLEEP", "10")))
elif mode == "fail":
    print(json.dumps({"event": "session.error"}))
    sys.exit(3)
print(json.dumps({"event": "session.end"}))
'''


def sha256(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def make_workspace(base: Path) -> Path:
    ws = Path(tempfile.mkdtemp(dir=str(base), prefix="ws_"))
    subprocess.run(["git", "init"], cwd=str(ws), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    subprocess.run(["git", "config", "user.email", "t@t.com"], cwd=str(ws), check=True)
    subprocess.run(["git", "config", "user.name", "t"], cwd=str(ws), check=True)
    (ws / "README.md").write_text("init")
    (ws / "owned.txt").write_text("hello world")
    subprocess.run(["git", "add", "."], cwd=str(ws), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=str(ws), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    return ws


class TestMuseCliWorker(unittest.TestCase):
    def setUp(self):
        self.scratch_root = Path(tempfile.mkdtemp(dir=str(SCRATCH_BASE), prefix="sr_"))
        self.scratch_root.chmod(0o700)
        self.fake_muse = self.scratch_root / "fake_muse.py"
        self.fake_muse.write_text(FAKE_MUSE_SOURCE)
        self.fake_muse.chmod(0o755)
        self.contract = self.scratch_root / "contract.md"
        self.contract.write_text("Change owned.txt from hello to hi.")
        os.chmod(self.contract, 0o600)

    def tearDown(self):
        try:
            shutil.rmtree(str(self.scratch_root))
        except Exception:
            pass

    def run_lane(self, ws, cid, mode=None, env_extra=None, extra=None, owned=None,
                 checks=None, apply_patch=None):
        cmd = [sys.executable, WRAPPER, "--root", str(ws), "--contract-id", cid,
               "--scratch-root", str(self.scratch_root), "--muse-bin", str(self.fake_muse)]
        if apply_patch is None:
            cmd.extend(["--contract", str(self.contract)])
        else:
            cmd.extend(["--apply-patch", str(apply_patch)])
        for o in (owned or ["owned.txt"]):
            cmd.extend(["--owned-path", o])
        for ch in (checks or []):
            cmd.extend(["--check-json", json.dumps(ch)])
        if extra:
            cmd.extend(extra)
        env = os.environ.copy()
        helper_tmp = SCRATCH_BASE / f"helper_{uuid.uuid4().hex[:6]}"
        helper_tmp.mkdir(parents=True, exist_ok=True)
        env["TMPDIR"] = str(helper_tmp)
        if mode is not None:
            env["FAKE_MUSE_MODE"] = mode
        if env_extra:
            env.update(env_extra)
        return subprocess.run(cmd, capture_output=True, text=True, timeout=60, env=env, cwd=str(REPO_ROOT))

    def evidence_dir(self, res):
        import re
        m = re.search(r"evidence:\s*(\S+)", res.stdout + res.stderr)
        self.assertIsNotNone(m, res.stdout + res.stderr)
        return Path(m.group(1))

    def test_success_edit_then_apply(self):
        ws = make_workspace(self.scratch_root)
        edits = [{"path": "owned.txt", "content": "hi world"}]
        dump = self.scratch_root / "argv_dump.json"
        res = self.run_lane(ws, f"ok-{uuid.uuid4().hex[:4]}", mode="edit",
                            env_extra={"FAKE_MUSE_EDITS": json.dumps(edits),
                                       "FAKE_MUSE_ARGV_DUMP": str(dump)},
                            checks=[["python3", "-c", "import sys; sys.exit(0)"]])
        self.assertEqual(res.returncode, 0, res.stdout + res.stderr)
        ev = self.evidence_dir(res)
        # main repo untouched by the run itself
        self.assertEqual((ws / "owned.txt").read_text(), "hello world")
        patch = ev / "patch.diff"
        self.assertTrue(patch.exists())
        meta = json.loads((ev / "patch.meta.json").read_text())
        self.assertEqual(meta["changed"], ["owned.txt"])
        # yolo/headless flags actually reached the muse invocation
        rec = json.loads(dump.read_text())
        argv = rec["argv"]
        for flag in ("exec", "--json", "--yolo", "--user-input-auto-resolve",
                     "--no-foreign-personal-context", "--disable-web-tools"):
            self.assertIn(flag, argv, argv)
        self.assertIn("muse-spark-1.2-contributor", argv)
        self.assertIn("xhigh", argv)
        self.assertIn("Change owned.txt from hello to hi.", rec["prompt"])
        self.assertIn("owned.txt", rec["prompt"])
        # the agent ran inside a scratch worktree, not the main repo
        self.assertNotEqual(os.path.realpath(rec["cwd"]), os.path.realpath(str(ws)))
        self.assertIn("muse-cli-worktrees", rec["cwd"])
        # worktree cleaned up after success
        self.assertFalse(Path(rec["cwd"]).exists())
        # evidence is private
        self.assertEqual(oct(ev.stat().st_mode)[-3:], "700")
        # apply the reviewed patch
        res2 = self.run_lane(ws, f"apply-{uuid.uuid4().hex[:4]}", apply_patch=patch)
        self.assertEqual(res2.returncode, 0, res2.stdout + res2.stderr)
        self.assertEqual((ws / "owned.txt").read_text(), "hi world")

    def test_noop_fail_closed_and_allow_empty(self):
        ws = make_workspace(self.scratch_root)
        res = self.run_lane(ws, f"noop-{uuid.uuid4().hex[:4]}", mode="noop")
        self.assertEqual(res.returncode, 33, res.stdout + res.stderr)
        res2 = self.run_lane(ws, f"noopok-{uuid.uuid4().hex[:4]}", mode="noop", extra=["--allow-empty"])
        self.assertEqual(res2.returncode, 0, res2.stdout + res2.stderr)

    def test_timeout_kills_and_preserves_main_repo(self):
        ws = make_workspace(self.scratch_root)
        t0 = time.monotonic()
        res = self.run_lane(ws, f"to-{uuid.uuid4().hex[:4]}", mode="sleep",
                            env_extra={"FAKE_MUSE_SLEEP": "30"}, extra=["--wall-seconds", "3"])
        self.assertEqual(res.returncode, 32, res.stdout + res.stderr)
        self.assertLess(time.monotonic() - t0, 25)
        self.assertEqual((ws / "owned.txt").read_text(), "hello world")

    def test_muse_failure(self):
        ws = make_workspace(self.scratch_root)
        res = self.run_lane(ws, f"fail-{uuid.uuid4().hex[:4]}", mode="fail")
        self.assertEqual(res.returncode, 31, res.stdout + res.stderr)

    def test_outside_owned_scope_violation(self):
        ws = make_workspace(self.scratch_root)
        res = self.run_lane(ws, f"scope-{uuid.uuid4().hex[:4]}", mode="outside")
        self.assertEqual(res.returncode, 34, res.stdout + res.stderr)
        self.assertFalse((ws / "outside_cli.txt").exists())
        # patch kept in evidence for inspection
        ev = self.evidence_dir(res)
        self.assertTrue((ev / "patch.diff").exists())

    def test_main_repo_contamination(self):
        ws = make_workspace(self.scratch_root)
        res = self.run_lane(ws, f"cont-{uuid.uuid4().hex[:4]}", mode="contaminate",
                            env_extra={"FAKE_MUSE_MAIN_ROOT": str(ws)})
        self.assertEqual(res.returncode, 35, res.stdout + res.stderr)

    def test_validation_failure_and_timeout(self):
        ws = make_workspace(self.scratch_root)
        edits = [{"path": "owned.txt", "content": "hi world"}]
        res = self.run_lane(ws, f"vfail-{uuid.uuid4().hex[:4]}", mode="edit",
                            env_extra={"FAKE_MUSE_EDITS": json.dumps(edits)},
                            checks=[["python3", "-c", "import sys; sys.exit(1)"]])
        self.assertEqual(res.returncode, 22, res.stdout + res.stderr)
        res2 = self.run_lane(ws, f"vto-{uuid.uuid4().hex[:4]}", mode="edit",
                             env_extra={"FAKE_MUSE_EDITS": json.dumps(edits)},
                             checks=[["python3", "-c", "import time; time.sleep(30)"]],
                             extra=["--wall-seconds", "5"])
        self.assertEqual(res2.returncode, 23, res2.stdout + res2.stderr)
        self.assertEqual((ws / "owned.txt").read_text(), "hello world")

    def test_shell_string_check_rejected(self):
        ws = make_workspace(self.scratch_root)
        res = self.run_lane(ws, f"shchk-{uuid.uuid4().hex[:4]}", mode="edit",
                            env_extra={"FAKE_MUSE_EDITS": json.dumps([{"path": "owned.txt", "content": "x"}])},
                            checks=[["bash", "-c", "echo hi"]])
        self.assertEqual(res.returncode, 10, res.stdout + res.stderr)

    def test_initial_ceiling(self):
        ws = make_workspace(self.scratch_root)
        cid = f"ceil-{uuid.uuid4().hex[:4]}"
        for i in range(3):
            self.contract.write_text(f"noop contract {i}")
            res = self.run_lane(ws, cid, mode="noop", extra=["--allow-empty"])
            self.assertEqual(res.returncode, 0, f"i={i} {res.stdout + res.stderr}")
        self.contract.write_text("noop contract final")
        res = self.run_lane(ws, cid, mode="noop", extra=["--allow-empty"])
        self.assertEqual(res.returncode, 14, res.stdout + res.stderr)

    def test_suppression_of_unchanged_failure(self):
        ws = make_workspace(self.scratch_root)
        cid = f"sup-{uuid.uuid4().hex[:4]}"
        res1 = self.run_lane(ws, cid, mode="outside")
        self.assertEqual(res1.returncode, 34)
        res2 = self.run_lane(ws, cid, mode="outside")
        self.assertEqual(res2.returncode, 13, res2.stdout + res2.stderr)

    def test_apply_rejects_outside_owned(self):
        ws = make_workspace(self.scratch_root)
        # craft a patch touching an un-owned path via a throwaway clone
        ws2 = make_workspace(self.scratch_root)
        (ws2 / "other.txt").write_text("new file")
        subprocess.run(["git", "add", "-A"], cwd=str(ws2), check=True, stdout=subprocess.DEVNULL)
        patch_bytes = subprocess.run(["git", "diff", "--cached", "--binary", "--no-renames"],
                                     cwd=str(ws2), capture_output=True, check=True).stdout
        patch = self.scratch_root / "outside.diff"
        patch.write_bytes(patch_bytes)
        res = self.run_lane(ws, f"apscope-{uuid.uuid4().hex[:4]}", apply_patch=patch, owned=["owned.txt"])
        self.assertEqual(res.returncode, 41, res.stdout + res.stderr)
        self.assertFalse((ws / "other.txt").exists())

    def test_apply_invalid_patch(self):
        ws = make_workspace(self.scratch_root)
        bad = self.scratch_root / "bad.diff"
        bad.write_text("this is not a patch")
        res = self.run_lane(ws, f"apinv-{uuid.uuid4().hex[:4]}", apply_patch=bad)
        self.assertEqual(res.returncode, 40, res.stdout + res.stderr)

    def test_apply_tab_filename_cannot_bypass_scope(self):
        # a filename containing a literal tab must not truncate at the tab and
        # sneak past the owned-path gate
        ws = make_workspace(self.scratch_root)
        ws2 = make_workspace(self.scratch_root)
        (ws2 / "owned.txt\tx").write_text("evil")
        subprocess.run(["git", "add", "-A"], cwd=str(ws2), check=True, stdout=subprocess.DEVNULL)
        patch_bytes = subprocess.run(["git", "diff", "--cached", "--binary", "--no-renames"],
                                     cwd=str(ws2), capture_output=True, check=True).stdout
        patch = self.scratch_root / "tabname.diff"
        patch.write_bytes(patch_bytes)
        res = self.run_lane(ws, f"aptab-{uuid.uuid4().hex[:4]}", apply_patch=patch, owned=["owned.txt"])
        self.assertEqual(res.returncode, 41, res.stdout + res.stderr)
        self.assertFalse((ws / "owned.txt\tx").exists())

    def test_apply_base_mismatch_fails_closed(self):
        ws = make_workspace(self.scratch_root)
        edits = [{"path": "owned.txt", "content": "hi world"}]
        res = self.run_lane(ws, f"apbase-{uuid.uuid4().hex[:4]}", mode="edit",
                            env_extra={"FAKE_MUSE_EDITS": json.dumps(edits)})
        self.assertEqual(res.returncode, 0, res.stdout + res.stderr)
        patch = self.evidence_dir(res) / "patch.diff"
        # move HEAD with an unrelated commit
        (ws / "unrelated.txt").write_text("x")
        subprocess.run(["git", "add", "-A"], cwd=str(ws), check=True, stdout=subprocess.DEVNULL)
        subprocess.run(["git", "commit", "-qm", "move head"], cwd=str(ws), check=True, stdout=subprocess.DEVNULL)
        res2 = self.run_lane(ws, f"apbase2-{uuid.uuid4().hex[:4]}", apply_patch=patch)
        self.assertEqual(res2.returncode, 40, res2.stdout + res2.stderr)
        self.assertEqual((ws / "owned.txt").read_text(), "hello world")
        res3 = self.run_lane(ws, f"apbase3-{uuid.uuid4().hex[:4]}", apply_patch=patch,
                             extra=["--allow-base-mismatch"])
        self.assertEqual(res3.returncode, 0, res3.stdout + res3.stderr)
        self.assertEqual((ws / "owned.txt").read_text(), "hi world")

    def test_apply_runs_checks_and_rolls_back_on_failure(self):
        ws = make_workspace(self.scratch_root)
        edits = [{"path": "owned.txt", "content": "hi world"}]
        res = self.run_lane(ws, f"apchk-{uuid.uuid4().hex[:4]}", mode="edit",
                            env_extra={"FAKE_MUSE_EDITS": json.dumps(edits)})
        self.assertEqual(res.returncode, 0, res.stdout + res.stderr)
        patch = self.evidence_dir(res) / "patch.diff"
        res2 = self.run_lane(ws, f"apchk2-{uuid.uuid4().hex[:4]}", apply_patch=patch,
                             checks=[["python3", "-c", "import sys; sys.exit(1)"]])
        self.assertEqual(res2.returncode, 22, res2.stdout + res2.stderr)
        self.assertEqual((ws / "owned.txt").read_text(), "hello world")
        res3 = self.run_lane(ws, f"apchk3-{uuid.uuid4().hex[:4]}", apply_patch=patch,
                             checks=[["python3", "-c", "import sys; sys.exit(0)"]])
        self.assertEqual(res3.returncode, 0, res3.stdout + res3.stderr)
        self.assertEqual((ws / "owned.txt").read_text(), "hi world")

    def test_lock_contention(self):
        # the lock is anchored at the CANONICAL scratch root, independent of
        # the caller-narrowed --scratch-root, so both lanes exclude each other
        ws = make_workspace(self.scratch_root)
        canonical = Path(os.path.realpath("/lustre/nvwulf/scratch/anadgeri/codex-cache"))
        lock_dir = canonical / "muse-worker-locks"
        lock_dir.mkdir(parents=True, exist_ok=True)
        repo_hash = hashlib.sha256(os.path.realpath(os.path.abspath(str(ws))).encode()).hexdigest()[:16]
        fd = os.open(str(lock_dir / f"repo-{repo_hash}.lock"), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            res = self.run_lane(ws, f"lock-{uuid.uuid4().hex[:4]}", mode="noop", extra=["--allow-empty"])
            self.assertEqual(res.returncode, 12, res.stdout + res.stderr)
        finally:
            os.close(fd)

    def test_scratch_env_override(self):
        alt_root = Path(f"/lustre/nvwulf/scratch/anadgeri/muse-cli-alt-root-{uuid.uuid4().hex[:8]}")
        alt_scratch = alt_root / "sub"
        try:
            ws = make_workspace(self.scratch_root)
            # without the env override, a scratch outside the default canonical
            # root is rejected at preflight
            cmd = [sys.executable, WRAPPER, "--root", str(ws), "--contract-id", "altroot",
                   "--contract", str(self.contract), "--owned-path", "owned.txt",
                   "--scratch-root", str(alt_scratch), "--muse-bin", str(self.fake_muse), "--allow-empty"]
            env = os.environ.copy()
            env["FAKE_MUSE_MODE"] = "noop"
            env.pop("MUSE_WORKER_SCRATCH_ROOT", None)
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=60, env=env, cwd=str(REPO_ROOT))
            self.assertEqual(res.returncode, 10, res.stdout + res.stderr)
            # with MUSE_WORKER_SCRATCH_ROOT the same invocation succeeds
            env["MUSE_WORKER_SCRATCH_ROOT"] = str(alt_root)
            res2 = subprocess.run(cmd, capture_output=True, text=True, timeout=60, env=env, cwd=str(REPO_ROOT))
            self.assertEqual(res2.returncode, 0, res2.stdout + res2.stderr)
            # /tmp can never be the scratch, env override or not
            env["MUSE_WORKER_SCRATCH_ROOT"] = "/tmp"
            cmd_tmp = list(cmd)
            cmd_tmp[cmd_tmp.index(str(alt_scratch))] = "/tmp/x"
            res3 = subprocess.run(cmd_tmp, capture_output=True, text=True, timeout=60, env=env, cwd=str(REPO_ROOT))
            self.assertEqual(res3.returncode, 10, res3.stdout + res3.stderr)
        finally:
            shutil.rmtree(str(alt_root), ignore_errors=True)

    def test_committed_work_captured(self):
        # a yolo agent that commits in the worktree must not lose its work
        ws = make_workspace(self.scratch_root)
        edits = [{"path": "owned.txt", "content": "hi world"}]
        res = self.run_lane(ws, f"commit-{uuid.uuid4().hex[:4]}", mode="commit",
                            env_extra={"FAKE_MUSE_EDITS": json.dumps(edits)})
        self.assertEqual(res.returncode, 0, res.stdout + res.stderr)
        ev = self.evidence_dir(res)
        meta = json.loads((ev / "patch.meta.json").read_text())
        self.assertEqual(meta["changed"], ["owned.txt"])
        self.assertIn(b"hi world", (ev / "patch.diff").read_bytes())

    def test_contamination_detected_even_when_muse_fails(self):
        ws = make_workspace(self.scratch_root)
        res = self.run_lane(ws, f"contfail-{uuid.uuid4().hex[:4]}", mode="contaminate_fail",
                            env_extra={"FAKE_MUSE_MAIN_ROOT": str(ws)})
        self.assertEqual(res.returncode, 35, res.stdout + res.stderr)

    def test_timeout_does_not_suppress_retry(self):
        ws = make_workspace(self.scratch_root)
        cid = f"transient-{uuid.uuid4().hex[:4]}"
        res = self.run_lane(ws, cid, mode="sleep", env_extra={"FAKE_MUSE_SLEEP": "30"},
                            extra=["--wall-seconds", "3"])
        self.assertEqual(res.returncode, 32, res.stdout + res.stderr)
        # identical fingerprint (same contract/owned/wall/steps) must not be suppressed
        edits = [{"path": "owned.txt", "content": "hi world"}]
        res2 = self.run_lane(ws, cid, mode="edit",
                             env_extra={"FAKE_MUSE_EDITS": json.dumps(edits)},
                             extra=["--wall-seconds", "3"])
        self.assertEqual(res2.returncode, 0, res2.stdout + res2.stderr)

    def test_missing_muse_binary(self):
        ws = make_workspace(self.scratch_root)
        cmd = [sys.executable, WRAPPER, "--root", str(ws), "--contract-id", "nobin",
               "--contract", str(self.contract), "--owned-path", "owned.txt",
               "--scratch-root", str(self.scratch_root), "--muse-bin", "/nonexistent/muse"]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=30, cwd=str(REPO_ROOT))
        self.assertEqual(res.returncode, 30, res.stdout + res.stderr)


if __name__ == "__main__":
    unittest.main()
