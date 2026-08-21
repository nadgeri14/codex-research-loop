# Muse wrapper production correction v1

## Objective

Make the Meta Muse Spark direct-API implementation path production-safe and make the repository package, installer, and installed-path contract internally consistent. This is research-loop infrastructure only; do not change scientific experiment code, data, state, or decisions.

## Ownership

You may edit only:

- `scripts/muse_research_worker.py`
- `tests/test_muse_research_worker.py`
- `scripts/install_codex_research_loop.py`
- installer tests under `tests/` that directly exercise `scripts/install_codex_research_loop.py`
- `codex-research-loop/skill/research-loop/SKILL.md`
- `codex-research-loop/skill/research-loop/references/muse-spark.md`
- `codex-research-loop/agents/research-lead.toml`
- `codex-research-loop/agents/research-code-reviewer.toml` only if terminology must remain consistent
- `codex-research-loop/AGENTS.block.md`
- `codex-research-loop/README.md`

Do not edit any user-wide installed file under `~/.codex` or `~/.agents`; installation occurs only after independent review. Do not edit experiment files. Other agents and the user have changes in this worktree: preserve them and do not revert unrelated work.

## Required corrections

1. Consolidate `scripts/muse_research_worker.py` so every top-level function has one authoritative definition. Do not leave later definitions overriding safer earlier definitions. Ensure compatibility with the installed Python 3.13 runtime; do not rely on unavailable `os.openat`.
2. Production authentication must locate the existing Muse credential at `~/.config/muse/auth.json`, require safe file permissions, accept only the documented `providers.meta.api_key` or `providers.meta.access_token` shapes, never print or persist the secret, and not select unrelated OpenAI-shaped auth files first. Test-only credential and endpoint overrides must remain loopback/test-name restricted.
3. Parse the real Meta Responses envelope, including reasoning items and `message.content` output-text blocks. Reject tool calls and ambiguous/multiple proposal payloads. Keep a strict size budget. Add fixtures representative of the production envelope.
4. For implementation attempts, require at least one valid proposed change and at least one actual repository content change. A no-op or an edit whose result equals current content must fail unless a new explicit caller flag authorizes no-op; default must be fail-closed. Do not falsely report success with `applied: []`.
5. Route edits through a tested rollback-capable transaction. A failure or interruption after any staged/committed operation must restore all owned paths to their exact pre-run state. Add an injected mid-commit failure test.
6. Stream validation stdout/stderr to bounded files under the canonical scratch root instead of buffering unbounded output in RAM. Persist only bounded evidence, report truncation explicitly, enforce per-check and aggregate limits, terminate process groups on timeout, and clean up resistant children.
7. Strengthen before/after repository scope identity. Reject unsafe validation commands before execution; validation must not be able to delete `.git`, alter outside-owned paths, invoke a shell string (`bash -c`, `sh -c`, etc.), or escape through symlinks. Detect changes to pre-existing dirty outside-owned files. Scope violation must fail closed without damaging the repository.
8. Enforce finite argument limits, maximum check count, aggregate argv bytes, input/output/response limits, wall/HTTP timeouts, correction ceilings, and lock/state integrity.
9. Update repository package instructions and agent TOMLs so every location selects the reviewed direct API worker and never raw `muse exec`. The model remains exactly `muse-spark-1.2-contributor` with `xhigh` reasoning; no substitute model, model tools, shell tools, or subagents.
10. Update the installer transactionally so it installs/exposes the worker at a stable user-wide path, synchronizes both `~/.codex/skills/research-loop` and `~/.agents/skills/research-loop` (or safely retires the duplicate), updates all relevant agent instructions consistently, preserves rollback/backups, and verifies installed content. Do not install during this task.

## Stable behavior

- Preserve the documented CLI unless a backward-compatible option is added.
- Preserve canonical scratch use under `/lustre/nvwulf/scratch/anadgeri/codex-cache`; never use `/tmp` or `/dev/shm` for caches, evidence, downloads, environments, or build artifacts.
- Preserve private permissions: wrapper-owned directories `0700`, evidence and credential-derived files `0600`.
- The model proposes bounded `replace`/`create`/`delete` operations only. The caller chooses validation.
- Do not contact any endpoint other than `https://api.meta.ai/v1/responses` in production.
- Never expose credentials in stdout, stderr, prompts, request evidence, exceptions, process argv, or environment captures.

## Acceptance criteria

- All existing wrapper tests pass after being updated only where old behavior was unsafe.
- New tests cover actual Muse credential discovery/precedence, production Responses envelope parsing, rejected empty/no-op proposals, mid-commit rollback, bounded validation capture, pre-execution rejection of dangerous validation argv, dirty outside-owned mutation detection, installer synchronization, stable worker path, and install rollback.
- `python -m py_compile scripts/muse_research_worker.py tests/test_muse_research_worker.py scripts/install_codex_research_loop.py` passes.
- `python -m unittest -v tests.test_muse_research_worker` passes.
- Relevant installer tests pass.
- `git diff --check` passes for every owned path.
- Return a concise implementation handoff listing changed files and exact checks run. Do not claim production readiness; independent Sol-high review decides that.

## Boundary

If any requirement needs a scientific or experimental decision, stop and report the exact ambiguity. Do not choose research behavior.
