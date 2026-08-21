# Token-efficient research loop — Phase 1 direct API

- Apply these defaults to long-running scientific or ML research, autoresearch, iterative training/evaluation, benchmark optimization, hypothesis testing, and cluster experiment series. Do not activate for ordinary one-off coding.
- Use the user-wide `research-loop` skill. Start/resume from `research-manager handoff --json` when `.research/state.json` exists; init once otherwise. Never reconstruct from Codex transcripts.
- Treat caps as context budgets, never storage limits. Raw evidence stays untouched. Use `research-manager inspect RUN_ID --section evidence|summary|health|all --json` when detail matters.
- Use GPT-5.6-Sol at `xhigh` for diagnosis, hypothesis, design, planning, specs, interpretation, go/no-go. Use GPT-5.6-Sol at `high` read-only for diff/validation review vs. contract.
- After Sol-xhigh defines ownership/constraints/criteria, delegate all execution-ready code/config/test changes to Meta Muse Spark (`muse-spark-1.2-contributor` at `xhigh`) via `scripts/muse_research_worker.py` — the reviewed direct API wrapper, not raw `muse exec`. Example: `python scripts/muse_research_worker.py --root REPO --contract CONTRACT --owned-path REL --context-path REL --check-json '["cmd"]' --scratch-root /lustre/nvwulf/scratch/anadgeri/codex-cache --wall-seconds N --http-seconds N --max-input-bytes N --max-output-tokens N --max-response-bytes N --attempt-kind initial|correction --contract-id ID`. Sol-high reviews every diff/validation; unfavorable → correction back to Muse Spark via wrapper; ambiguity → Sol-xhigh revises contract. Muse Spark is sole implementation worker; if unavailable, stop without substitution. No user-wide install; installer not changed in Phase 1. Budgets finite, scratch canonical under `/lustre/nvwulf/scratch/anadgeri/codex-cache` (no `/tmp`/`/dev/shm`), evidence `0700`/`0600`, no model tools/shell tools/subagents. Muse Spark remains sole model, CLI layer bypassed.
- Save tokens via scripts/bounded output/change-only/compact handoffs, not lower reasoning.
- Never use an LLM to wait for deterministic conditions. Arm `research-manager arm-monitor`, hand its blocking command to one `cluster_monitor` with no history, end frontier continuation.
- On Slurm, use `cluster-manager` with persistent `--until wake --timeout 0`; ordinary progress stays inside deterministic process.
- Route evidence: deterministic first, Luna-low only for ambiguous material events, frontier only for scientific reasoning.
- Route one-off checks to Luna-low `cluster_checker`; it reports once, never polls or diagnoses science.
- Use `research-manager health RUN_ID --json` after initial progress, on anomalies, at completion, before interpretation. Warning/critical → Sol-xhigh.
- Cluster monitoring is experiment-read-only; never submit/cancel/requeue/edit or choose science steps.
- If a recurring operation is missing, update shared manager once and reinstall.
- Recover from plans/results, git state, evidence, live jobs. Do not load Codex JSONL.
