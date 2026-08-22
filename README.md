# Portable Codex research loop — native Muse Spark subagent

This package installs a token-efficient research workflow in which GPT-5.6-Sol
at `xhigh` remains the main scientific brain and contract author. The native
Codex custom agent `muse_implementor`, pinned to
`meta/muse-spark-1.2-contributor` at `xhigh`, performs bounded implementation.
GPT-5.6-Sol at `high` reviews every implementation diff read-only. Deterministic
tools and GPT-5.6-Luna at `low` own operational observation.

```text
Sol-xhigh research lead
        |
        v
bounded contract + exact ownership
        |
        v
native muse_implementor (Muse Spark xhigh)
        |
        v
Sol-high read-only review
        |
        v
Sol-xhigh launch / scientific decision
        |
        v
deterministic cluster-manager watch
        +-- ordinary progress: sleep silently
        +-- ambiguous material event: bounded Luna triage
        `-- scientific/material event: wake Sol-xhigh
```

The main thread never changes to Muse. Muse receives no inherited research
conversation, only a compact implementation contract, exact owned/context
paths, and approved validation commands. It cannot make scientific decisions,
operate cluster jobs, or spawn more agents. One write-capable implementor runs
at a time, and the Sol-high reviewer gates consequential launches.

The monitor persists cursors and compact state atomically, deduplicates events,
applies deterministic invariants, and never returns merely because time passed.
A long job with only expected progress produces no frontier continuation.

## Requirements

- Python 3.10+; the managers use only the standard library.
- Slurm for scheduler-specific `cluster-manager` operations.
- A current Codex release with custom subagents enabled.
- The local model router running with the Meta provider authenticated and
  `meta/muse-spark-1.2-contributor` present in its catalog.

The package does not install provider credentials, change router credentials,
or rewrite the user's default main model.

The repository intentionally contains no API keys, access tokens, router
capability URLs, user paths, model transcripts, research evidence, or cluster
logs.

## Included

- Five native Codex custom-agent profiles: Sol-xhigh research lead, Muse-xhigh
  implementor, Sol-high read-only reviewer, Luna-low monitor, and Luna-low
  checker.
- The token-efficient `research-loop` skill and concise managed `AGENTS.md`
  policy block.
- Restart-safe `research-manager` and deterministic, incremental
  `cluster-manager` monitoring CLIs.
- An idempotent installer that preserves unrelated user configuration and
  creates recoverable backups before replacing an existing target.
- Unit and integration tests for installation, durable state, incremental log
  cursors, event routing, deduplication, and zero-LLM unchanged waits.
- The optional codex-router Meta `agent_message` compatibility patch used by
  the verified native Muse smoke test.

## Install

```bash
git clone https://github.com/nadgeri14/codex-research-loop.git
cd codex-research-loop
python3 scripts/install_codex_research_loop.py --dry-run
python3 scripts/install_codex_research_loop.py
```

Ensure `$HOME/.local/bin` is on `PATH`, then fully restart Codex so it reloads
the custom-agent profiles and model catalog. The installer is idempotent and
backs up every changed existing target before replacing it.

It installs:

- `research-manager` and `cluster-manager` under `$HOME/.local/bin`;
- the user-wide `research-loop` skill under `$HOME/.agents/skills`;
- `research_lead` (Sol-xhigh), `muse_implementor` (Muse-xhigh),
  `research_code_reviewer` (Sol-high read-only), and the Luna-low operational
  agents under `${CODEX_HOME:-$HOME/.codex}/agents`;
- a concise marked policy block in `${CODEX_HOME:-$HOME/.codex}/AGENTS.md`,
  preserving all content outside that block.

Use `--codex-home`, `--skills-dir`, or `--bin-dir` to override destinations.
Use `--json` for machine-readable output.

For a project-scoped installation instead of a user-wide one, copy the agent
profiles into the target repository's `.codex/agents/` directory and append
`AGENTS.block.md` to that repository's `AGENTS.md`. See the official
[Codex subagent documentation](https://learn.chatgpt.com/docs/agent-configuration/subagents)
for custom-agent precedence and required fields.

## Meta router compatibility

The native Muse profile depends on codex-router exposing
`meta/muse-spark-1.2-contributor`. Router `0.4.0-beta.4` forwarded Codex's
internal `agent_message` item to Meta unchanged, which Meta rejected with
`input[n] did not match any supported type`. The tested, Meta-scoped repair is
stored at `patches/codex-router-meta-agent-message.patch`.

Apply it only when your installed router still has that bug:

```bash
git -C /path/to/codex-router apply --check \
  "$PWD/patches/codex-router-meta-agent-message.patch"
git -C /path/to/codex-router apply \
  "$PWD/patches/codex-router-meta-agent-message.patch"
```

Run the router checks and restart its service after applying it. Do not apply
the patch if `git apply --check` reports that the fix is already present or the
target version has diverged; inspect the newer router instead.

## Review boundary

- Sol-xhigh owns hypotheses, experiment design, contracts, scientific
  interpretation, and go/no-go.
- Muse-xhigh implements only the execution-ready contract within declared
  ownership and returns bounded validation evidence.
- Sol-high reviews the actual diff and checks for correctness, regressions,
  edge cases, and missing tests. It never edits or makes scientific decisions.
- Deterministic monitors wait; Luna-low classifies only bounded ambiguous
  operational events.

If the native Muse profile or routed model is unavailable, the workflow stops
without substituting another model or execution lane.

## Verify

```bash
python -m py_compile scripts/install_codex_research_loop.py tests/test_install_codex_research_loop.py
python -m unittest tests.test_install_codex_research_loop
python scripts/install_codex_research_loop.py --dry-run --json
git diff --check
```

Run the bounded end-to-end procedure in [docs/NATIVE_MUSE_SMOKE.md](docs/NATIVE_MUSE_SMOKE.md)
after configuring the Meta provider. The smoke writes only one exact 20-byte
artifact under a caller-selected scratch directory.

Research state is stored under each repository's `.research/` directory.
Bounded handoffs are indexes only: full structured state remains available via
`research-manager inspect`, while raw logs, metrics, checkpoints, plots, and
artifacts remain untouched.
