# Portable Codex research loop

This package installs the token-efficient research workflow for every Codex conversation owned by the current user. It preserves the selected main model and reasoning effort for scientific work, while moving scheduler polling, log deltas, bookkeeping, comparisons, and training-health triage into deterministic tools.

It requires Python 3.10 or newer. `research-manager` works on any server;
the scheduler commands in `cluster-manager` require Slurm.

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
- the narrow `cluster_monitor` agent under `${CODEX_HOME:-$HOME/.codex}/agents`;
- a marked research-policy block in `${CODEX_HOME:-$HOME/.codex}/AGENTS.md`, preserving all content outside that block.

No main-model or main-reasoning setting is written. Only the read-only monitoring agent is pinned to the lower-cost `gpt-5.6-luna` model at low effort.

Use `--codex-home`, `--skills-dir`, or `--bin-dir` to override installation paths. Use `--json` for machine-readable output.

## Verify

```bash
research-manager --help
cluster-manager --help
python3 -m unittest tests.test_cluster_manager tests.test_research_manager tests.test_install_codex_research_loop
```

Research state is stored under each repository's `.research/` directory. Bounded handoffs are indexes only: full structured state remains available through `research-manager inspect`, and raw logs, metrics, checkpoints, plots, and artifacts are never truncated or rewritten.
