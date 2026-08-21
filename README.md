# codex-research-loop

A token-efficient, evidence-preserving research loop for Codex: GPT-5.6-Sol at
`xhigh` owns research reasoning, planning, and implementation contracts;
GPT-5.6-Sol at `high` (read-only) reviews every diff; Meta Muse Spark
(`muse-spark-1.2-contributor` at `xhigh`) is the sole implementation worker;
deterministic tools and GPT-5.6-Luna at `low` handle cluster observation and
long waits.

Muse Spark is reached through two reviewed lanes — never raw `muse exec`:

- **Direct API worker** (default): `scripts/muse_research_worker.py` — one-shot,
  fail-closed `POST https://api.meta.ai/v1/responses`, no tools, transactional
  owned-path edits, caller-declared validations, git-baseline scope
  verification, private evidence under scratch.
- **Gated CLI lane** (escalation only): `scripts/muse_cli_worker.py` — headless
  `muse exec --yolo --user-input-auto-resolve` inside a disposable git worktree
  under scratch, hard wall deadline, main-repo contamination check on every
  path, patch extracted for review, separate scope-verified `--apply-patch`
  step.

## Layout

- `scripts/` — the two Muse workers, `research_manager.py`,
  `cluster_manager.py`, and the installer
- `tests/` — stdlib-only test suites for all of the above (no model calls;
  fake server / fake muse binary)
- `codex-research-loop/` — the installable package: agent TOMLs, the
  user-wide `research-loop` skill, the managed `AGENTS.block.md`, and the
  package [README](codex-research-loop/README.md) with full install docs
- `review_contracts/` — the implementation-correction contracts this code was
  reviewed against

## Install

```bash
git clone https://github.com/nadgeri14/codex-research-loop.git
cd codex-research-loop
python3 scripts/install_codex_research_loop.py --dry-run
python3 scripts/install_codex_research_loop.py
```

Python 3.10+, standard library only. On a machine other than the original
cluster, set `MUSE_WORKER_SCRATCH_ROOT` to a local (non-tmpfs, non-`/tmp`)
scratch directory before running the workers.

## Verify

```bash
python3 -m unittest tests.test_muse_research_worker tests.test_muse_cli_worker \
  tests.test_research_manager tests.test_cluster_manager \
  tests.test_install_codex_research_loop tests.test_event_driven_monitor
```
