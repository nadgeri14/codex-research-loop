# Portable Codex research loop — Phase 1 direct Muse Spark API worker

This package installs the token-efficient research workflow for every Codex conversation owned by the current user. GPT-5.6-Sol at `xhigh` owns research reasoning, planning, hard diagnosis, interpretation, and decisions and creates the implementation contract. All code, configuration, and test review routes to GPT-5.6-Sol at `high` read-only (which may and must assess test and validation evidence for implementation correctness, but must not interpret scientific experiment results), execution-ready implementation routes to Meta Muse Spark (`muse-spark-1.2-contributor` at `xhigh`) via the reviewed direct API worker `scripts/muse_research_worker.py` — not raw `muse exec` — while routine cluster observation routes to deterministic tools or GPT-5.6-Luna at `low`. Muse Spark remains the sole implementation model; only the local CLI orchestration layer is bypassed. The Sol-xhigh research lead does not inspect raw code or implementation details; when an implementation detail could affect a research decision, the Sol-high reviewer provides a bounded, exact implementation-evidence handoff so the lead can revise the contract without reviewing code. The wrapper receives bounded owned/context file contents and finite caller-owned validation `check-json` argv arrays, calls `POST https://api.meta.ai/v1/responses` once with no tools, validates, applies transactionally, runs checks, verifies scope, and records private evidence. No substitution, no user-wide installation; installer not changed in Phase 1.

The waiting path is event driven:

```text
Sol-xhigh research -> implementation (direct API wrapper) -> Sol-high read-only review -> Sol-xhigh launch/decision -> durable handoff
                    |                                      |                 |                       |
                    |                                      |                 |                       v
                    |                                      |                 |         deterministic cluster-manager watch
                    |                                      |                 |             | routine progress: keep sleeping
                    |                                      |                 |             | ambiguous event: bounded Luna triage
                    |                                      |                 |             ` material/scientific event: Sol-xhigh wake
                    |                                      |                 |
                    |                                      |                 ` favorable: bounded verdict to Sol-xhigh; unfavorable: correction via wrapper; ambiguity: bounded evidence handoff for contract revision
                    |                                      ` Meta Muse Spark direct API (muse-spark-1.2-contributor at xhigh, no CLI/tools/subagents)
                    ` planning, hard diagnosis, contract, and launch/decision
```

The monitor persists cursors and compact state atomically, deduplicates events, applies invariants, and never returns on timer elapsed. A five-hour job with only expected progress produces no frontier continuations.

It requires Python 3.10+ (standard library only; no extra packages). `research-manager` works on any server; scheduler commands in `cluster-manager` require Slurm. Phase 1 additionally requires `scripts/muse_research_worker.py` and credentials at the existing Muse credential JSON (provider `meta` `api_key`/`access_token`); the wrapper is the sole operational path (raw `muse exec` not used) and does not install anything user-wide.

## Install on a new server

```bash
git clone https://github.com/nadgeri14/codex-research-loop.git
cd codex-research-loop
python3 scripts/install_codex_research_loop.py --dry-run
python3 scripts/install_codex_research_loop.py
```

Ensure `$HOME/.local/bin` is on `PATH`, then start a new Codex session. The installer is idempotent and backs up every changed existing target before replacing it.

It installs:

- `research-manager` and `cluster-manager` under `$HOME/.local/bin`;
- the user-wide `research-loop` skill under `$HOME/.agents/skills`;
- Sol-xhigh research-lead, Sol-high read-only research-code-reviewer, Luna-low one-off checker, and Luna-low long-monitor agents under `${CODEX_HOME:-$HOME/.codex}/agents`;
- a marked research-policy block in `${CODEX_HOME:-$HOME/.codex}/AGENTS.md`, preserving all content outside that block.

The installer does not rewrite the user's default main-model setting. Its research-lead agent is pinned to `gpt-5.6-sol` at `xhigh`, its code-reviewer agent to `gpt-5.6-sol` at `high` read-only, and its operational agents to `gpt-5.6-luna` at `low`. Meta Muse Spark (`muse-spark-1.2-contributor` at `xhigh`) is the sole implementation model and is invoked via the direct API worker `scripts/muse_research_worker.py` (caller-declared `owned-path`/`context-path`/`check-json`, finite budgets, canonical scratch `/lustre/nvwulf/scratch/anadgeri/codex-cache`, evidence `0700`/`0600`, `POST https://api.meta.ai/v1/responses` with `xhigh`, no tools). The installer is not changed in Phase 1 and package does not install Muse credentials or any user-wide packages.

Use `--codex-home`, `--skills-dir`, or `--bin-dir` to override installation paths. Use `--json` for machine-readable output.

## Verify

```bash
python -m py_compile scripts/muse_research_worker.py tests/test_muse_research_worker.py
python -m unittest tests.test_muse_research_worker
git diff --check -- scripts/muse_research_worker.py tests/test_muse_research_worker.py codex-research-loop/skill/research-loop/SKILL.md codex-research-loop/skill/research-loop/references/muse-spark.md codex-research-loop/AGENTS.block.md codex-research-loop/README.md
```

Research state is stored under each repository's `.research/` directory. Bounded handoffs are indexes only: full structured state remains available through `research-manager inspect`, and raw logs, metrics, checkpoints, plots, and artifacts are never truncated or rewritten.
