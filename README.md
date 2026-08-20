# Portable Codex research loop

This package installs the token-efficient research workflow for every Codex conversation owned by the current user. GPT-5.6-Sol at `xhigh` owns research reasoning, planning, hard diagnosis, interpretation, and decisions and creates the implementation contract. All code, configuration, and test review routes to GPT-5.6-Sol at `high` read-only (which may and must assess test and validation evidence for implementation correctness, but must not interpret scientific experiment results), execution-ready implementation routes to the locally installed Meta Muse Spark model `muse-spark-1.2-contributor` at `xhigh`, while routine cluster observation routes to deterministic tools or GPT-5.6-Luna at `low`. Muse Spark is the sole implementation worker for research code, configuration, and test changes. The Sol-xhigh research lead does not inspect raw code or implementation details; when an implementation detail could affect a research decision, the Sol-high reviewer provides a bounded, exact implementation-evidence handoff (with file/line references and the relevant behavior) so the lead can resolve only the scientific question or revise the contract without reviewing code.

The waiting path is event driven:

```text
Sol-xhigh research -> implementation -> Sol-high read-only review -> Sol-xhigh launch/decision -> durable handoff
                    |               |                 |                       |
                    |               |                 |                       v
                    |               |                 |         deterministic cluster-manager watch
                    |               |                 |             | routine progress: keep sleeping
                    |               |                 |             | ambiguous event: bounded Luna triage
                    |               |                 |             ` material/scientific event: Sol-xhigh wake
                    |               |                 |
                    |               |                 ` favorable: bounded verdict (file/line handoff) to Sol-xhigh; unfavorable: correction to Muse; ambiguity: bounded evidence handoff to Sol-xhigh for contract revision
                    |               ` Meta Muse Spark (muse-spark-1.2-contributor at xhigh)
                    ` planning, hard diagnosis, contract, and launch/decision (no code review; receives bounded handoff from Sol-high reviewer)
```

The monitor persists inode/offset cursors and compact state atomically, deduplicates events, applies configurable invariants, and never returns because a timer elapsed. A five-hour job with only expected progress therefore produces no frontier continuations and no Luna classifications between launch and the first material event.

It requires Python 3.10 or newer. `research-manager` works on any server;
the scheduler commands in `cluster-manager` require Slurm. The Muse implementation route additionally requires an authenticated `muse` command on `PATH` with access to `muse-spark-1.2-contributor` at `xhigh`; this package does not install Muse or its credentials and Muse remains an external prerequisite.

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

The installer does not rewrite the user's default main-model setting. Its research-lead agent is pinned to `gpt-5.6-sol` at `xhigh`, its code-reviewer agent to `gpt-5.6-sol` at `high` read-only, and its operational agents to `gpt-5.6-luna` at `low`. Meta Muse Spark (`muse-spark-1.2-contributor` at `xhigh`) is the sole implementation path and is not installed by this package; it is invoked through the existing Muse CLI at its explicit model ID and remains an external prerequisite. Only the long monitor may change deterministic cursor state while observing a run; the one-off checker and the code reviewer are read-only.

Use `--codex-home`, `--skills-dir`, or `--bin-dir` to override installation paths. Use `--json` for machine-readable output.

## Verify

```bash
research-manager --help
cluster-manager --help
python3 -m unittest tests.test_cluster_manager tests.test_event_driven_monitor \
  tests.test_research_manager tests.test_install_codex_research_loop
```

Research state is stored under each repository's `.research/` directory. Bounded handoffs are indexes only: full structured state remains available through `research-manager inspect`, and raw logs, metrics, checkpoints, plots, and artifacts are never truncated or rewritten.
