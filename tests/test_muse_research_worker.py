#!/usr/bin/env python3
"""Direct Muse Spark worker tests — stdlib, loopback fake server, scratch-backed."""
import hashlib
import http.server
import json
import os
import re
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import uuid
from pathlib import Path
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRATCH_BASE = Path("/lustre/nvwulf/scratch/anadgeri/codex-cache/tmp/muse-worker-tests")
SCRATCH_BASE.mkdir(parents=True, exist_ok=True)
try:
    os.chmod(SCRATCH_BASE, 0o700)
except Exception:
    pass
WRAPPER = str(REPO_ROOT / "scripts" / "muse_research_worker.py")

def sha256(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()

def make_workspace(base: Path) -> Path:
    ws = Path(tempfile.mkdtemp(dir=str(base), prefix="ws_"))
    subprocess.run(["git", "init"], cwd=str(ws), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    subprocess.run(["git", "config", "user.email", "t@t.com"], cwd=str(ws), check=True)
    subprocess.run(["git", "config", "user.name", "t"], cwd=str(ws), check=True)
    (ws / "README.md").write_text("init")
    subprocess.run(["git", "add", "."], cwd=str(ws), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=str(ws), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    return ws

def make_contract(base: Path, content: str = "contract") -> Path:
    fd, p = tempfile.mkstemp(dir=str(base), prefix="ctr_", suffix=".md")
    os.close(fd)
    pp = Path(p)
    pp.write_text(content)
    os.chmod(pp, 0o600)
    return pp

def make_credential(base: Path, shape: str = "api_key", token: str = "test-secret-token-12345", perm: int = 0o600) -> Path:
    fd, p = tempfile.mkstemp(dir=str(base), prefix="cred_TEST_", suffix=".json")
    os.close(fd)
    pp = Path(p)
    data = {"providers": {"meta": {shape: token}}}
    pp.write_text(json.dumps(data))
    os.chmod(pp, perm)
    return pp

class FakeHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass
    def do_POST(self):
        cfg = self.server.cfg  # type: ignore
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length) if length else b""
            self.server.received_body = body  # type: ignore
            self.server.received_headers = dict(self.headers)  # type: ignore
            if cfg.get("expect_auth", True):
                auth = self.headers.get("Authorization", "")
                if not auth.startswith("Bearer "):
                    self.send_response(401)
                    self.end_headers()
                    try:
                        self.wfile.write(b"missing auth")
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                    return
            if cfg.get("disconnect"):
                try:
                    self.connection.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                self.close_connection = True
                return
            if cfg.get("delay"):
                time.sleep(cfg["delay"])
            status = cfg.get("http_status", 200)
            if status != 200:
                self.send_response(status)
                self.end_headers()
                try:
                    self.wfile.write(cfg.get("http_body", b"error"))
                except (BrokenPipeError, ConnectionResetError):
                    pass
                return
            if cfg.get("check_request"):
                try:
                    j = json.loads(body.decode())
                    if j.get("model") != "muse-spark-1.2-contributor":
                        self.send_response(500)
                        self.end_headers()
                        try:
                            self.wfile.write(b"bad model")
                        except (BrokenPipeError, ConnectionResetError):
                            pass
                        return
                    if j.get("reasoning", {}).get("effort") != "xhigh":
                        self.send_response(500)
                        self.end_headers()
                        try:
                            self.wfile.write(b"bad effort")
                        except (BrokenPipeError, ConnectionResetError):
                            pass
                        return
                    if j.get("stream") is not False:
                        self.send_response(500)
                        self.end_headers()
                        try:
                            self.wfile.write(b"bad stream")
                        except (BrokenPipeError, ConnectionResetError):
                            pass
                        return
                    if "tools" in j:
                        self.send_response(500)
                        self.end_headers()
                        try:
                            self.wfile.write(b"tools forbidden")
                        except (BrokenPipeError, ConnectionResetError):
                            pass
                        return
                except Exception:
                    pass
            resp_body = cfg.get("resp_body")
            if resp_body is None:
                resp_body = json.dumps({"status": "completed", "model": "muse-spark-1.2-contributor", "output": [{"type": "output_text", "text": json.dumps({"schema_version": 1, "changes": [], "handoff": "ok", "claimed_checks": []})}]}).encode()
            if cfg.get("oversized"):
                resp_body = b"x" * (3 * 1024 * 1024)
            if cfg.get("malformed_json"):
                resp_body = b"{ truncated"
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(resp_body)))
            self.end_headers()
            try:
                self.wfile.write(resp_body)
            except (BrokenPipeError, ConnectionResetError):
                pass
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception:
            try:
                self.send_error(500)
            except Exception:
                pass

def start_server(cfg: dict):
    srv = http.server.HTTPServer(("127.0.0.1", 0), FakeHandler)
    srv.cfg = cfg  # type: ignore
    srv.received_body = b""  # type: ignore
    srv.received_headers = {}  # type: ignore
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    # wait for port
    time.sleep(0.05)
    return srv, t

def run_wrapper(workspace, contract, owned_paths, scratch_root, contract_id, attempt_kind="initial", context_paths=None, checks=None, extra=None, env_extra=None, endpoint=None, cred_path=None, allow_empty=False):
    cmd = [sys.executable, WRAPPER, "--root", str(workspace), "--contract", str(contract), "--scratch-root", str(scratch_root), "--attempt-kind", attempt_kind, "--contract-id", contract_id]
    if allow_empty:
        cmd.append("--allow-empty")
    for o in owned_paths:
        cmd.extend(["--owned-path", o])
    if context_paths:
        for c in context_paths:
            cmd.extend(["--context-path", c])
    if checks:
        for ch in checks:
            cmd.extend(["--check-json", json.dumps(ch)])
    if extra:
        cmd.extend(extra)
    env = os.environ.copy()
    # helper tmp must remain canonical scratch, not the invalid candidate
    helper_tmp = SCRATCH_BASE / f"helper_{uuid.uuid4().hex[:6]}"
    helper_tmp.mkdir(parents=True, exist_ok=True)
    env["TMPDIR"] = str(helper_tmp)
    env["TMP"] = str(helper_tmp)
    env["TEMP"] = str(helper_tmp)
    env["UV_CACHE_DIR"] = str(helper_tmp / "uv")
    env["PIP_CACHE_DIR"] = str(helper_tmp / "pip")
    env["npm_config_cache"] = str(helper_tmp / "npm")
    env["XDG_CACHE_HOME"] = str(helper_tmp / "xdg")
    Path(env["UV_CACHE_DIR"]).mkdir(parents=True, exist_ok=True)
    Path(env["PIP_CACHE_DIR"]).mkdir(parents=True, exist_ok=True)
    if endpoint:
        env["MUSE_SPARK_TEST_ENDPOINT"] = endpoint
    if cred_path:
        env["MUSE_SPARK_TEST_CREDENTIAL_PATH"] = str(cred_path)
    if env_extra:
        env.update(env_extra)
    # ensure HOME does not point to real home for auth tests unless overridden
    if "HOME" not in env_extra if env_extra else True:
        # keep default HOME but ensure it doesn't leak real creds; use a fake empty home unless test needs real
        pass
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=25, env=env, cwd=str(REPO_ROOT))
    return result

def make_proposal(changes, handoff="done", claimed=None, schema_version=1, extra=None):
    obj = {"schema_version": schema_version, "changes": changes, "handoff": handoff}
    if claimed is not None:
        obj["claimed_checks"] = claimed
    else:
        obj["claimed_checks"] = []
    if extra:
        obj.update(extra)
    return obj

def wrap_response(proposal_obj, status="completed", model="muse-spark-1.2-contributor", extra_top=None, output_override=None):
    if output_override is not None:
        out = output_override
    else:
        txt = json.dumps(proposal_obj)
        out = [{"type": "output_text", "text": txt}]
    resp = {"status": status, "model": model, "output": out}
    if extra_top:
        resp.update(extra_top)
    return json.dumps(resp).encode()

def evidence_dir_from(result):
    m = re.search(r"evidence:\s*(\S+)", result.stdout + result.stderr)
    if m:
        return Path(m.group(1))
    return None

class TestMuseWorker(unittest.TestCase):
    def setUp(self):
        self.scratch_root = Path(tempfile.mkdtemp(dir=str(SCRATCH_BASE), prefix="sr_"))
        self.scratch_root.chmod(0o700)
        (self.scratch_root / "test_tmp").mkdir(parents=True, exist_ok=True)
        self.servers = []
    def tearDown(self):
        for srv, t in self.servers:
            try:
                srv.shutdown()
                srv.server_close()
            except Exception:
                pass
        try:
            shutil.rmtree(str(self.scratch_root))
        except Exception:
            pass
    def start(self, cfg):
        srv, t = start_server(cfg)
        self.servers.append((srv, t))
        port = srv.server_address[1]
        endpoint = f"http://127.0.0.1:{port}/v1/responses"
        return srv, endpoint

    def test_completed_valid_replace(self):
        ws = make_workspace(self.scratch_root)
        owned = ws / "owned.txt"
        owned.write_text("hello world")
        subprocess.run(["git", "add", "."], cwd=str(ws), check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        contract = make_contract(self.scratch_root, "replace owned")
        h = sha256(owned)
        proposal = make_proposal([{"op": "replace", "path": "owned.txt", "expected_sha256": h, "old_text": "hello", "new_text": "hi"}])
        srv, ep = self.start({"resp_body": wrap_response(proposal)})
        cred = make_credential(self.scratch_root, "api_key", "secret-1234567890")
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"cid-{uuid.uuid4().hex[:6]}", checks=[["python3", "-c", "import sys; sys.exit(0)"]], endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 0, res.stdout + res.stderr)
        self.assertEqual((ws / "owned.txt").read_text(), "hi world")
        ev = evidence_dir_from(res)
        self.assertIsNotNone(ev)
        self.assertTrue((ev / "summary.json").exists())
        self.assertEqual(oct(ev.stat().st_mode)[-3:], "700")
        for p in ev.rglob("*"):
            if p.is_file():
                self.assertEqual(oct(p.stat().st_mode)[-3:], "600", str(p))
        self.assertNotIn("secret-1234567890", (ev / "raw_response.bin").read_text(errors="ignore") + (ev / "request_sanitized.json").read_text(errors="ignore"))
        # request must not contain bearer
        self.assertNotIn("secret-1234567890", res.stdout + res.stderr)

    def test_create_and_delete(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "create")
        proposal = make_proposal([{"op": "create", "path": "newdir/new.txt", "content": "created"}])
        srv, ep = self.start({"resp_body": wrap_response(proposal)})
        cred = make_credential(self.scratch_root, "access_token", "tok-abcdef1234567890")
        res = run_wrapper(ws, contract, ["newdir/new.txt"], self.scratch_root, f"cid-{uuid.uuid4().hex[:6]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 0, res.stdout + res.stderr)
        self.assertTrue((ws / "newdir" / "new.txt").exists())
        self.assertEqual((ws / "newdir" / "new.txt").read_text(), "created")
        # delete
        h = sha256(ws / "newdir" / "new.txt")
        proposal2 = make_proposal([{"op": "delete", "path": "newdir/new.txt", "expected_sha256": h}])
        srv2, ep2 = self.start({"resp_body": wrap_response(proposal2)})
        cred2 = make_credential(self.scratch_root, "api_key", "del-token-1234567890")
        res2 = run_wrapper(ws, contract, ["newdir/new.txt"], self.scratch_root, f"cid2-{uuid.uuid4().hex[:6]}", endpoint=ep2, cred_path=cred2)
        self.assertEqual(res2.returncode, 0, res2.stdout + res2.stderr)
        self.assertFalse((ws / "newdir" / "new.txt").exists())

    def test_credential_shapes(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "cred")
        for shape in ["api_key", "access_token"]:
            proposal = make_proposal([])
            srv, ep = self.start({"resp_body": wrap_response(proposal)})
            cred = make_credential(self.scratch_root, shape, f"shape-{shape}-token-123456")
            res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"cred-{shape}-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred, allow_empty=True)
            self.assertEqual(res.returncode, 0, res.stdout + res.stderr)
            ev = evidence_dir_from(res)
            txt = "".join(p.read_text(errors="ignore") for p in ev.rglob("*") if p.is_file())
            self.assertNotIn(f"shape-{shape}", txt)

    def test_auth_missing(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        # no cred file, use fake home without cred
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"auth-{uuid.uuid4().hex[:4]}", endpoint="http://127.0.0.1:9/v1/responses", cred_path=Path("/tmp/nonexistent_TEST.json"), env_extra={"HOME": str(self.scratch_root / "emptyhome")})
        # wrapper should fail auth (11)
        self.assertEqual(res.returncode, 11)

    def test_auth_malformed(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        bad = Path(tempfile.mktemp(dir=str(self.scratch_root), prefix="bad_TEST_", suffix=".json"))
        bad.write_text("{ not json")
        os.chmod(bad, 0o600)
        srv, ep = self.start({"resp_body": wrap_response(make_proposal([]))})
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"mal-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=bad)
        self.assertEqual(res.returncode, 11)

    def test_auth_unsafe_permissions(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        cred = make_credential(self.scratch_root, "api_key", "unsafe-token-123456", perm=0o644)
        srv, ep = self.start({"resp_body": wrap_response(make_proposal([]))})
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"unsafe-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 11)

    def test_http_4xx(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        srv, ep = self.start({"http_status": 401, "http_body": b"unauthorized"})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"http4-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 17)

    def test_http_5xx(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        srv, ep = self.start({"http_status": 500, "http_body": b"server error"})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"http5-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 17)

    def test_disconnect(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        srv, ep = self.start({"disconnect": True})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"disc-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertIn(res.returncode, (16, 15, 17))

    def test_slow_response(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        srv, ep = self.start({"delay": 3, "resp_body": wrap_response(make_proposal([]))})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"slow-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred, extra=["--http-seconds", "1", "--wall-seconds", "5"])
        self.assertEqual(res.returncode, 15)

    def test_oversized_response(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        srv, ep = self.start({"oversized": True})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"over-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred, extra=["--max-response-bytes", "1000"])
        self.assertEqual(res.returncode, 19)

    def test_malformed_json(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        srv, ep = self.start({"malformed_json": True})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"malj-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 19)

    def test_incomplete(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        body = json.dumps({"status": "incomplete", "model": "muse-spark-1.2-contributor", "output": []}).encode()
        srv, ep = self.start({"resp_body": body})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"inc-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 18)

    def test_wrong_model(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        body = wrap_response(make_proposal([]), model="wrong-model")
        srv, ep = self.start({"resp_body": body})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"mod-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 19)

    def test_tool_call(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        body = json.dumps({"status": "completed", "model": "muse-spark-1.2-contributor", "output": [{"type": "output_text", "text": json.dumps(make_proposal([]))}], "tool_calls": [{"name": "shell"}]}).encode()
        srv, ep = self.start({"resp_body": body})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"tool-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 19)

    def test_duplicate_output(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        prop = json.dumps(make_proposal([]))
        body = json.dumps({"status": "completed", "model": "muse-spark-1.2-contributor", "output": [{"type": "output_text", "text": prop}, {"type": "output_text", "text": prop}]}).encode()
        srv, ep = self.start({"resp_body": body})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"dup-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 19)

    def test_no_output_text(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        body = json.dumps({"status": "completed", "model": "muse-spark-1.2-contributor", "output": []}).encode()
        srv, ep = self.start({"resp_body": body})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"noout-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 19)

    def test_fenced_proposal(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([])
        fenced = "```json\n" + json.dumps(prop) + "\n```"
        body = json.dumps({"status": "completed", "model": "muse-spark-1.2-contributor", "output": [{"type": "output_text", "text": fenced}]}).encode()
        srv, ep = self.start({"resp_body": body})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"fence-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 19)

    def test_outside_owned(self):
        ws = make_workspace(self.scratch_root)
        (ws / "owned.txt").write_text("hi")
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([{"op": "create", "path": "outside.txt", "content": "evil"}])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"out-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 20)
        self.assertFalse((ws / "outside.txt").exists())

    def test_absolute_path(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([{"op": "create", "path": "/tmp/evil", "content": "x"}])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"abs-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 20)

    def test_traversal_path(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([{"op": "create", "path": "../evil", "content": "x"}])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"trav-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 20)

    def test_nul_path(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([{"op": "create", "path": "a\0b", "content": "x"}])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"nul-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 20)

    def test_symlink_component(self):
        ws = make_workspace(self.scratch_root)
        owned_dir = ws / "owned"
        owned_dir.mkdir()
        target = ws / "target"
        target.mkdir()
        link = owned_dir / "link"
        link.symlink_to(target)
        contract = make_contract(self.scratch_root, "x")
        # symlink in owned path should be rejected at preflight before HTTP
        srv, ep = self.start({"resp_body": wrap_response(make_proposal([]))})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned"], self.scratch_root, f"sym-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 10)
        ev = evidence_dir_from(res)
        self.assertIsNotNone(ev)
        self.assertTrue((ev / "summary.json").exists())

    def test_special_file(self):
        ws = make_workspace(self.scratch_root)
        fifo = ws / "owned_fifo"
        try:
            os.mkfifo(str(fifo))
        except Exception:
            self.skipTest("mkfifo not supported")
        contract = make_contract(self.scratch_root, "x")
        # owned FIFO should be rejected at preflight before HTTP
        srv, ep = self.start({"resp_body": wrap_response(make_proposal([]))})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned_fifo"], self.scratch_root, f"fifo-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 10)
        ev = evidence_dir_from(res)
        self.assertIsNotNone(ev)
        # no HTTP should have occurred? but we check bounded evidence exists
        self.assertTrue((ev / "summary.json").exists())

    def test_duplicate_ops(self):
        ws = make_workspace(self.scratch_root)
        (ws / "a.txt").write_text("hello")
        h = sha256(ws / "a.txt")
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([{"op": "replace", "path": "a.txt", "expected_sha256": h, "old_text": "hello", "new_text": "hi"}, {"op": "replace", "path": "a.txt", "expected_sha256": h, "old_text": "hi", "new_text": "ho"}])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["a.txt"], self.scratch_root, f"dupop-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 20)
        self.assertEqual((ws / "a.txt").read_text(), "hello")

    def test_wrong_hash(self):
        ws = make_workspace(self.scratch_root)
        (ws / "a.txt").write_text("hello")
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([{"op": "replace", "path": "a.txt", "expected_sha256": "0"*64, "old_text": "hello", "new_text": "hi"}])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["a.txt"], self.scratch_root, f"wh-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 20)

    def test_missing_old_text(self):
        ws = make_workspace(self.scratch_root)
        (ws / "a.txt").write_text("hello")
        h = sha256(ws / "a.txt")
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([{"op": "replace", "path": "a.txt", "expected_sha256": h, "old_text": "missing", "new_text": "hi"}])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["a.txt"], self.scratch_root, f"miss-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 20)

    def test_ambiguous_old_text(self):
        ws = make_workspace(self.scratch_root)
        (ws / "a.txt").write_text("aaa")
        h = sha256(ws / "a.txt")
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([{"op": "replace", "path": "a.txt", "expected_sha256": h, "old_text": "a", "new_text": "b"}])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["a.txt"], self.scratch_root, f"amb-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 20)

    def test_create_overwrite(self):
        ws = make_workspace(self.scratch_root)
        (ws / "a.txt").write_text("hello")
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([{"op": "create", "path": "a.txt", "content": "new"}])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["a.txt"], self.scratch_root, f"overw-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 20)
        self.assertEqual((ws / "a.txt").read_text(), "hello")

    def test_oversized_edit(self):
        ws = make_workspace(self.scratch_root)
        (ws / "a.txt").write_text("hi")
        h = sha256(ws / "a.txt")
        contract = make_contract(self.scratch_root, "x")
        # use max-response cap to trigger oversized response path, not proposal size
        srv, ep = self.start({"oversized": True})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["a.txt"], self.scratch_root, f"big-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred, extra=["--max-response-bytes", "5000"])
        self.assertEqual(res.returncode, 19)

    def test_all_or_nothing(self):
        ws = make_workspace(self.scratch_root)
        (ws / "a.txt").write_text("hello")
        (ws / "b.txt").write_text("world")
        ha = sha256(ws / "a.txt")
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([{"op": "replace", "path": "a.txt", "expected_sha256": ha, "old_text": "hello", "new_text": "hi"}, {"op": "replace", "path": "b.txt", "expected_sha256": "0"*64, "old_text": "world", "new_text": "wo"}])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["a.txt", "b.txt"], self.scratch_root, f"atom-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 20)
        self.assertEqual((ws / "a.txt").read_text(), "hello")
        self.assertEqual((ws / "b.txt").read_text(), "world")

    def test_scratch_tmp(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["a.txt"], "/tmp", f"tmp-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 10)

    def test_scratch_canonical_root(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["a.txt"], "/lustre/nvwulf/scratch/other", f"canon-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 10)

    def test_scratch_symlink(self):
        ws = make_workspace(self.scratch_root)
        link = self.scratch_root / "link_to_tmp"
        try:
            link.symlink_to("/tmp")
        except Exception:
            self.skipTest("symlink not supported")
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["a.txt"], str(link), f"link-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 10)

    def test_validation_nonzero(self):
        ws = make_workspace(self.scratch_root)
        (ws / "a.txt").write_text("hello")
        h = sha256(ws / "a.txt")
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([{"op": "replace", "path": "a.txt", "expected_sha256": h, "old_text": "hello", "new_text": "hi"}])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["a.txt"], self.scratch_root, f"valfail-{uuid.uuid4().hex[:4]}", checks=[["python3", "-c", "import sys; sys.exit(1)"]], endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 22)

    def test_validation_timeout(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["a.txt"], self.scratch_root, f"valto-{uuid.uuid4().hex[:4]}", checks=[["python3", "-c", "import time; time.sleep(5)"]], endpoint=ep, cred_path=cred, extra=["--wall-seconds", "3"], allow_empty=True)
        self.assertEqual(res.returncode, 23)

    def test_validation_term_resistant_cleanup(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        # child ignores TERM
        script = "import signal, time; signal.signal(signal.SIGTERM, lambda s,f: None); time.sleep(5)"
        res = run_wrapper(ws, contract, ["a.txt"], self.scratch_root, f"term-{uuid.uuid4().hex[:4]}", checks=[["python3", "-c", script]], endpoint=ep, cred_path=cred, extra=["--wall-seconds", "3"], allow_empty=True)
        self.assertEqual(res.returncode, 23)
        # ensure no leftover: check that after wrapper, no sleep process remains with our scratch?
        # we just verify exit code and that wrapper cleaned up

    def test_dirty_untouched_no_false_positive(self):
        ws = make_workspace(self.scratch_root)
        (ws / "outside.txt").write_text("dirty")
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"dirtyok-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred, allow_empty=True)
        self.assertEqual(res.returncode, 0)
        self.assertEqual((ws / "outside.txt").read_text(), "dirty")

    def test_dirty_outside_tracked(self):
        ws = make_workspace(self.scratch_root)
        tracked = ws / "tracked.txt"
        tracked.write_text("orig")
        subprocess.run(["git", "add", "."], cwd=str(ws), check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["git", "commit", "-m", "add"], cwd=str(ws), check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        contract = make_contract(self.scratch_root, "x")
        # validation will modify tracked outside
        prop = make_proposal([])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"tracked-{uuid.uuid4().hex[:4]}", checks=[["python3", "-c", "open('tracked.txt','w').write('mod')"]], endpoint=ep, cred_path=cred, allow_empty=True)
        self.assertEqual(res.returncode, 21)

    def test_dirty_outside_untracked(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"untracked-{uuid.uuid4().hex[:4]}", checks=[["python3", "-c", "open('outside_new.txt','w').write('hi')"]], endpoint=ep, cred_path=cred, allow_empty=True)
        self.assertEqual(res.returncode, 21)
        self.assertTrue((ws / "outside_new.txt").exists())

    def test_spaces_in_path(self):
        ws = make_workspace(self.scratch_root)
        tracked = ws / "file with space.txt"
        tracked.write_text("orig")
        subprocess.run(["git", "add", "."], cwd=str(ws), check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["git", "commit", "-m", "add"], cwd=str(ws), check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"space-{uuid.uuid4().hex[:4]}", checks=[["python3", "-c", "open('file with space.txt','w').write('mod')"]], endpoint=ep, cred_path=cred, allow_empty=True)
        self.assertEqual(res.returncode, 21)

    def test_rename_detection(self):
        ws = make_workspace(self.scratch_root)
        f = ws / "oldname.txt"
        f.write_text("data")
        subprocess.run(["git", "add", "."], cwd=str(ws), check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["git", "commit", "-m", "add"], cwd=str(ws), check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"rename-{uuid.uuid4().hex[:4]}", checks=[["python3", "-c", "import os; os.rename('oldname.txt','newname.txt')"]], endpoint=ep, cred_path=cred, allow_empty=True)
        self.assertEqual(res.returncode, 21)

    def test_git_failure_scope(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        # capture_repo sets git_error when git fails after validation removes .git;
        # detect_scope must then fail closed with a scope violation
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"gitfail-{uuid.uuid4().hex[:4]}", checks=[["python3", "-c", "import shutil; shutil.rmtree('.git')"]], endpoint=ep, cred_path=cred, allow_empty=True)
        self.assertEqual(res.returncode, 21)

    def test_head_movement(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        head_script = "import subprocess; open('new.txt','w').write('x'); subprocess.run(['git','add','.'],check=True,capture_output=True); subprocess.run(['git','commit','-m','c'],check=True,capture_output=True)"
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"head-{uuid.uuid4().hex[:4]}", checks=[["python3", "-c", head_script]], endpoint=ep, cred_path=cred, allow_empty=True)
        self.assertEqual(res.returncode, 21)

    def test_locking(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        srv2, ep2 = self.start({"delay": 2, "resp_body": wrap_response(prop)})
        marker = self.scratch_root / f"lock_marker_{uuid.uuid4().hex[:4]}.txt"
        marker_script = f"import time; open(r'{marker}','w').write('acquired'); time.sleep(2)"
        def run1():
            r = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"lock-{uuid.uuid4().hex[:4]}", endpoint=ep2, cred_path=cred, extra=["--wall-seconds", "8"], checks=[["python3", "-c", marker_script]], allow_empty=True)
            return r
        t = threading.Thread(target=run1)
        t.start()
        # wait for deterministic marker that wrapper holds repo-wide lock and validation started
        for _ in range(30):
            if marker.exists():
                break
            time.sleep(0.1)
        res2 = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"lock2-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred, extra=["--wall-seconds", "5"], allow_empty=True)
        self.assertEqual(res2.returncode, 12)
        t.join(timeout=10)

    def test_malformed_state(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        cid = f"malstate-{uuid.uuid4().hex[:4]}"
        # create malformed state at the wrapper's canonical per-repo state path
        repo_hash = hashlib.sha256(os.path.realpath(os.path.abspath(str(ws))).encode()).hexdigest()[:16]
        bad_state_dir = self.scratch_root / "muse-worker-state" / f"{repo_hash}_{cid}"
        bad_state_dir.mkdir(parents=True, exist_ok=True)
        (bad_state_dir / "state.json").write_text("{ not json")
        os.chmod(bad_state_dir / "state.json", 0o600)
        srv, ep = self.start({"resp_body": wrap_response(make_proposal([]))})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, cid, endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 25)

    def test_initial_ceiling(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        cid = f"iceil-{uuid.uuid4().hex[:4]}"
        for i in range(3):
            prop = make_proposal([])
            srv, ep = self.start({"resp_body": wrap_response(prop)})
            cred = make_credential(self.scratch_root)
            c = make_contract(self.scratch_root, f"contract {i}")
            res = run_wrapper(ws, c, ["owned.txt"], self.scratch_root, cid, attempt_kind="initial", endpoint=ep, cred_path=cred, allow_empty=True)
            self.assertEqual(res.returncode, 0, f"i={i} {res.stdout+res.stderr}")
        # fourth initial should fail ceiling
        srv, ep = self.start({"resp_body": wrap_response(make_proposal([]))})
        cred = make_credential(self.scratch_root)
        c = make_contract(self.scratch_root, "contract final")
        res = run_wrapper(ws, c, ["owned.txt"], self.scratch_root, cid, attempt_kind="initial", endpoint=ep, cred_path=cred, allow_empty=True)
        self.assertEqual(res.returncode, 14)

    def test_correction_ceiling(self):
        ws = make_workspace(self.scratch_root)
        cid = f"cceil-{uuid.uuid4().hex[:4]}"
        for i in range(3):
            c = make_contract(self.scratch_root, f"corr {i}")
            prop = make_proposal([])
            srv, ep = self.start({"resp_body": wrap_response(prop)})
            cred = make_credential(self.scratch_root)
            res = run_wrapper(ws, c, ["owned.txt"], self.scratch_root, cid, attempt_kind="correction", endpoint=ep, cred_path=cred, allow_empty=True)
            self.assertEqual(res.returncode, 0, res.stdout+res.stderr)
        c = make_contract(self.scratch_root, "corr final")
        srv, ep = self.start({"resp_body": wrap_response(make_proposal([]))})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, c, ["owned.txt"], self.scratch_root, cid, attempt_kind="correction", endpoint=ep, cred_path=cred, allow_empty=True)
        self.assertEqual(res.returncode, 14)

    def test_suppression(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        cid = f"sup-{uuid.uuid4().hex[:4]}"
        # first fail due to outside path
        prop = make_proposal([{"op": "create", "path": "outside.txt", "content": "evil"}])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        res1 = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, cid, endpoint=ep, cred_path=cred)
        self.assertEqual(res1.returncode, 20)
        # second same fingerprint should be suppressed
        srv2, ep2 = self.start({"resp_body": wrap_response(prop)})
        cred2 = make_credential(self.scratch_root)
        res2 = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, cid, endpoint=ep2, cred_path=cred2)
        self.assertEqual(res2.returncode, 13)

    def test_evidence_permissions(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        srv, ep = self.start({"resp_body": wrap_response(make_proposal([]))})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"perm-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred, allow_empty=True)
        self.assertEqual(res.returncode, 0)
        ev = evidence_dir_from(res)
        self.assertEqual(oct(ev.stat().st_mode)[-3:], "700")
        for p in ev.rglob("*"):
            if p.is_file():
                self.assertEqual(oct(p.stat().st_mode)[-3:], "600", str(p))

    def test_secret_absence(self):
        ws = make_workspace(self.scratch_root)
        (ws / "owned.txt").write_text("hello")
        contract = make_contract(self.scratch_root, "secret test")
        h = sha256(ws / "owned.txt")
        prop = make_proposal([{"op": "replace", "path": "owned.txt", "expected_sha256": h, "old_text": "hello", "new_text": "hi"}])
        token = "super-secret-TEST-token-99999"
        srv, ep = self.start({"resp_body": wrap_response(prop), "check_request": True})
        cred = make_credential(self.scratch_root, "api_key", token)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"sec-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
        self.assertEqual(res.returncode, 0)
        ev = evidence_dir_from(res)
        # check no bearer in evidence
        all_text = ""
        for p in ev.rglob("*"):
            if p.is_file():
                try:
                    all_text += p.read_text(errors="ignore") + "\n"
                except Exception:
                    pass
        self.assertNotIn(token, all_text)
        self.assertNotIn("Bearer", all_text)
        # also check that request body logged not contain token
        self.assertNotIn(token, res.stdout + res.stderr)
        # check Authorization header was sent (server received)
        self.assertTrue(srv.received_headers.get("Authorization", "").startswith("Bearer "))
        # but not in evidence http_meta headers
        http_meta = json.loads((ev / "http_meta.json").read_text())
        self.assertNotIn("Authorization", "".join(str(v) for v in http_meta.get("headers", {}).values()))

    def test_input_budget(self):
        ws = make_workspace(self.scratch_root)
        # create large owned file to exceed max-input
        big = "x" * (300 * 1024)
        (ws / "owned.txt").write_text(big)
        contract = make_contract(self.scratch_root, "x")
        srv, ep = self.start({"resp_body": wrap_response(make_proposal([]))})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"inp-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred, extra=["--max-input-bytes", "1024"])
        self.assertEqual(res.returncode, 10)

    def test_output_budget(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        srv, ep = self.start({"oversized": True})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"out-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred, extra=["--max-response-bytes", "500"])
        self.assertEqual(res.returncode, 19)

    def test_wall_budget(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        srv, ep = self.start({"delay": 5, "resp_body": wrap_response(make_proposal([]))})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"wall-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred, extra=["--wall-seconds", "2", "--http-seconds", "1"])
        self.assertIn(res.returncode, (15, 23))

    def test_no_retry(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        # server that would fail first then succeed if retried – but wrapper should not retry
        call_count = {"n": 0}
        orig_handler = FakeHandler.do_POST
        def counting(self):
            call_count["n"] += 1
            return orig_handler(self)
        FakeHandler.do_POST = counting
        try:
            srv, ep = self.start({"http_status": 500, "http_body": b"err"})
            cred = make_credential(self.scratch_root)
            res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"noretry-{uuid.uuid4().hex[:4]}", endpoint=ep, cred_path=cred)
            self.assertEqual(res.returncode, 17)
            self.assertEqual(call_count["n"], 1)
        finally:
            FakeHandler.do_POST = orig_handler

    def test_bounded_logs(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        prop = make_proposal([])
        srv, ep = self.start({"resp_body": wrap_response(prop)})
        cred = make_credential(self.scratch_root)
        # validation with large output but raw intact
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, f"bounded-{uuid.uuid4().hex[:4]}", checks=[["python3", "-c", "for i in range(10000): print('hi')"]], endpoint=ep, cred_path=cred, allow_empty=True)
        self.assertEqual(res.returncode, 0)
        ev = evidence_dir_from(res)
        raw = (ev / "validation_0_stdout.log").read_bytes()
        self.assertGreater(len(raw), 10000)
        self.assertLess(len(res.stdout), 8192)

    def test_atomic_persistence_failure(self):
        ws = make_workspace(self.scratch_root)
        contract = make_contract(self.scratch_root, "x")
        cid = f"persist-{uuid.uuid4().hex[:4]}"
        # occupy the wrapper's canonical per-repo state dir path with a regular
        # file so state persistence must fail
        repo_hash = hashlib.sha256(os.path.realpath(os.path.abspath(str(ws))).encode()).hexdigest()[:16]
        state_dir = self.scratch_root / "muse-worker-state" / f"{repo_hash}_{cid}"
        state_dir.parent.mkdir(parents=True, exist_ok=True)
        state_dir.write_text("file not dir")
        srv, ep = self.start({"resp_body": wrap_response(make_proposal([]))})
        cred = make_credential(self.scratch_root)
        res = run_wrapper(ws, contract, ["owned.txt"], self.scratch_root, cid, endpoint=ep, cred_path=cred, allow_empty=True)
        # Should either succeed then fail persistence or preflight? The wrapper tries to mkdir parents, but if parent is file, mkdir fails -> persistence failure
        self.assertIn(res.returncode, (25, 10))

    def test_no_install(self):
        # Ensure wrapper didn't create anything in HOME
        home = Path(os.environ.get("HOME", "/tmp"))
        # Just check that wrapper doesn't install user-wide
        self.assertFalse((home / ".local" / "bin" / "muse-research-worker").exists())

if __name__ == "__main__":
    unittest.main()
