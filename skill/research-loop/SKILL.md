---
name: research-loop
description: Use for long-running scientific or ML research, autoresearch, iterative training/evaluation, benchmark optimization, hypothesis testing, training-health diagnosis, cluster experiments, or resuming such work across conversations. Provides an evidence-preserving, token-efficient loop with durable experiment state and silent-failure detection. Do not use for ordinary one-off coding, debugging, or a single short test unless it belongs to an experiment series.
---

# Research Loop

Preserve research quality by keeping scientific judgment, research planning, and decisions on GPT-5.6-Sol at `xhigh`; moving routine code, configuration, and test review to GPT-5.6-Sol at `high` read-only; moving execution-ready implementation to the native `muse_implementor` agent pinned to Meta Muse Spark (`meta/muse-spark-1.2-contributor` at `xhigh`); and keeping routine cluster observation on deterministic tools or Luna-low. Treat compact summaries as indexes to raw evidence, never as replacements for it.

## Start or resume

1. Work from the research repository or pass `--root REPO` explicitly.
2. If `.research/state.json` exists, begin with:

   ```bash
   research-manager handoff --json
   ```

   Use a non-login shell for manager commands when the execution tool supports it. This cluster's login-shell initialization can prepend module/logger noise to JSON.

3. If this is a new research program, initialize it once:

   ```bash
   research-manager init --objective "OBJECTIVE" --json
   ```

4. Recover from the handoff, current code, recorded evidence paths, and live job state. Do not load full Codex transcripts or entire historical logs into the active context.
5. Read [references/research-manager.md](references/research-manager.md) when exact command or JSON-schema details are needed.

## Route work by reasoning role

### Sol xhigh leads research

Use `gpt-5.6-sol` at `xhigh` for the main research thread and the configured `research_lead` agent. Preserve that model and effort for the hardest research problems; do not silently lower either one to save tokens.

Sol owns work that benefits from frontier research reasoning:

- scientific diagnosis and literature-grounded understanding;
- hypothesis formation and falsification criteria;
- baseline, treatment, metric, dataset, seed, and evaluation design;
- research strategy, experiment plans, and implementation specifications (the implementation contract);
- interpretation of results and anomalies;
- keep, revert, refine, confirm, redirect, and stop decisions.

Sol creates the implementation contract and does not directly implement research code, configuration, or test changes, including tiny changes, does not perform routine review of Muse diffs and validation reports, and does not inspect raw code or implementation details. When an implementation detail could affect a research decision, the reviewer provides a bounded, exact implementation-evidence handoff (with file/line references and the relevant behavior) so Sol can resolve only the scientific question or revise the contract without reviewing code. If the Muse Spark implementation path is unavailable or fails, stop and report the limitation; do not substitute Sol, Luna, Spark from another provider, or another model. Do not delegate scientific judgment to an implementation worker, cluster observer, `research-manager`, or `cluster-manager`.

### research_code_reviewer (Sol high, read-only) reviews implementation

Use `gpt-5.6-sol` at `high` with a `read-only` sandbox for the configured `research_code_reviewer` agent. It reviews every Muse diff and validation report against the Sol-xhigh implementation contract before a consequential launch or scientific conclusion.

- review-only: findings first, exact file/line references, correctness/regression/edge-case/test-coverage focus; assess test and validation evidence for implementation correctness;
- no edits and no new scientific decisions; do not interpret scientific experiment results;
- an unfavorable review produces a bounded correction request back to Muse for reimplementation; neither Sol-xhigh nor the reviewer patches the implementation itself;
- a scientific ambiguity is returned to the Sol-xhigh research lead with a bounded, exact implementation-evidence handoff (with file/line references and the relevant behavior) so the lead can resolve only the scientific question or revise the contract without reviewing code; Muse then reimplements and the reviewer reviews again;
- a favorable review produces a bounded review verdict/evidence handoff (with exact file/line references, how it aligns with the contract, residual risks, and validation evidence) to the Sol-xhigh research lead, which then owns launch/go-no-go and interpretation without duplicating routine review and without inspecting raw code.

The research lead does not inspect raw code or implementation details. When an implementation detail could affect a research decision, the reviewer provides a bounded, exact implementation-evidence handoff (with file/line references and the relevant behavior) so the lead can resolve only the scientific question or revise the contract without reviewing code. The reviewer may and must assess test and validation evidence for implementation correctness, but it must not interpret scientific experiment results or make scientific decisions.

### `muse_implementor` implements — sole implementation worker

After Sol defines intended behavior, ownership, constraints, and validation criteria, spawn the native Codex custom agent `muse_implementor`. That profile pins Meta Muse Spark (`meta/muse-spark-1.2-contributor`) at `xhigh` while retaining the Codex harness. Read [references/muse-spark.md](references/muse-spark.md) before delegation.

- spawn one write-capable `muse_implementor` at a time with no inherited conversation history; send only the compact current state, implementation contract, exact owned paths, optional bounded context paths, and caller-approved validation commands;
- Muse may use harness tools to inspect the declared paths, edit only owned paths, and run only the approved checks. It must preserve unrelated work, never spawn subagents, never operate cluster jobs, and never make a scientific decision;
- require a concise handoff containing changed paths, check commands/results, unresolved questions, and evidence references. Do not return full logs, transcripts, skills, or unchanged context;
- have the Sol-high read-only `research_code_reviewer` inspect every diff and validation handoff against the contract. An unfavorable verdict becomes a bounded correction task for a fresh Muse turn; scientific ambiguity returns to Sol-xhigh for a contract decision; a favorable verdict returns a bounded evidence handoff to Sol-xhigh;
- if the native profile or routed model is unavailable or fails, stop and report the limitation. Do not silently substitute Sol, Luna, another model, or another execution lane.

Do not assign overlapping files to concurrent implementation workers. If Muse encounters conflicting evidence, scope expansion, or a design choice that could change the experiment, it must stop at the boundary and return the question to Sol.

### Deterministic tools and Luna-low observe operations

Use deterministic tools for mechanical work:

- `research-manager` validates schemas, records state, synchronizes job states, aligns metrics, and emits bounded handoffs;
- `research-manager health RUN_ID --json` scans recorded logs for non-finite values, instability, stalled progress, throughput collapse, distributed/data/checkpoint failures, and other bounded health signals;
- `cluster-manager` reads Slurm, resource, GPU, and bounded log deltas, returning control when common training failures appear;
- the configured `cluster_monitor` may own one genuine long wait, but it receives only the exact armed monitor command and later any bounded ambiguous-event packet.

For a bounded one-off request such as “is the job still running smoothly?”, use the configured Luna-low `cluster_checker`. Give it only the relevant run IDs, process names, and bounded log paths. It may report scheduler/process/GPU/log health, but it must not diagnose scientific meaning, change experiments, or continue polling. Material anomalies return to Sol xhigh.

Use scripts directly for deterministic bookkeeping. Do not use Sol to tail logs, list processes, check routine scheduler state, or wait for a job.

## Suspend instead of waiting

Never use an LLM to wait for a deterministic external condition. After launching a long-running operation:

1. Persist its exact wake conditions with `research-manager arm-monitor RUN_ID ... --json`.
2. Give the returned blocking command to one `cluster_monitor` with no conversation history.
3. End the frontier continuation immediately. Do not call `wait_agent`, `list_agents`, repeat handoffs, or emit a status update merely because time passed.
4. Resume frontier reasoning only from the compact wake packet under `.research/monitors/RUN_ID/`.

The blocking `cluster-manager watch --until wake --timeout 0` process is Level 0. It owns scheduler polling, restart-safe log cursors, milestone detection, invariant checks, deduplication, and sleeping through ordinary progress without producing an assistant turn. Never use `--no-state`, `--emit-initial`, or a finite timeout for a long-running event monitor.

The Luna-low `cluster_monitor` is Level 1 only when the deterministic router emits `route: "LUNA"`. Give it only the bounded event packet, validate its structured classification with `cluster-manager resolve-luna`, and return to Level 0 when it classifies the event as routine. Luna must not make scientific decisions. Completion, scientific anomalies, and any `route: "SOL"` wake packet return to Sol xhigh at Level 2.

## Run one evidence-preserving iteration

### 1. Predeclare the experiment

State a falsifiable hypothesis and choose the smallest coherent change that tests it. Record:

- a matched baseline and treatment;
- one primary metric and its direction;
- success and failure criteria before seeing the result;
- the evaluation slice, sample count or seeds, and relevant controls;
- resource and time budgets when useful.

Create a spec from `research-manager template spec`, then register it with `research-manager plan SPEC.json`. Change one interpretable factor where practical. If several factors must change together, say why they form one coherent intervention.

### 2. Implement and validate

Have Sol-xhigh write the implementation contract, then delegate all execution-ready code, configuration, and test changes to the native `muse_implementor` agent. The Sol-high read-only reviewer reviews every resulting diff and validation handoff against the contract before launch; an unfavorable review becomes a bounded correction task for Muse and neither Sol agent patches the implementation; a scientific ambiguity returns to Sol-xhigh for contract revision; a favorable review produces a bounded verdict/evidence handoff to Sol-xhigh, which owns launch/go-no-go without duplicating routine review. If the native Muse profile or routed model is unavailable, stop without substitution or automatic wrapper fallback. Run focused unit, integration, and smoke checks appropriate to the risk. Record the checks actually completed:

```bash
research-manager validate RUN_ID --check "CHECK" --evidence PATH --json
```

Validation is a launch-safety gate, not scientific evidence of generalization. Never describe a smoke test, tiny overfit check, or single example as a benchmark gain.

### 3. Launch once and record once

Use the repository's reviewed launch path. `research-manager` never submits or cancels jobs. After submission, record the Slurm IDs and evidence locations:

```bash
research-manager record-launch RUN_ID --job-id JOB_ID --log LOG_PATH --artifact ARTIFACT_PATH --json
```

Do not repeatedly reconstruct `sbatch`, `squeue`, `sacct`, `scontrol`, `tail`, or log-search pipelines in conversation. If a generic operation is missing, improve the shared manager once and test it.

Arm the deterministic monitor immediately after recording the launch. Include only material wake conditions and the next scientific action:

```bash
research-manager arm-monitor RUN_ID --phase TRAINING --target-step TARGET \
  --checkpoint PATH --next-scientific-action "Interpret the completed milestone" --json
```

Pass the returned `monitor_command` unchanged to `cluster_monitor`, then stop the main continuation.

### 4. Wait outside the reasoning loop

For a one-off check, delegate to Luna-low `cluster_checker`, which should use one read-only `research-manager status --json` call or `cluster-manager status JOB_ID --no-state --logs --json` with bounded log bytes. For a genuine wait, use the armed event monitor and resume Sol only for a material wake packet, user request, or scientific decision. Ordinary steps, loss updates, log growth, an unchanged running state, and elapsed time are not wake conditions.

Never poll unchanged state in the main thread. Never feed routine elapsed-time updates or duplicate log tails back into the scientific context. A finite watch timeout is an operational error, not a reason to resume the frontier agent.

Run `research-manager health RUN_ID --json` after the first meaningful training progress, on a reported anomaly, at completion, and before interpreting results. A warning or critical signal must return control to Sol xhigh. Treat health checks as triage, not proof: inspect the cited raw log and relevant code before diagnosing, canceling, or changing the experiment.

### 5. Reduce before interpreting

Use deterministic project code to reduce raw outputs into a bounded summary. Include:

- primary metrics with uncertainty or distributional detail when available;
- deltas against the declared baseline;
- sample counts, seeds, and excluded or missing cases;
- wall time, accelerator time, and peak memory when relevant;
- failure modes and anomalies, including negative results;
- paths to full metrics, logs, artifacts, and plots;
- explicit health checks for non-finite values, step progress, throughput, gradients, distributed workers, the data pipeline, and checkpoint/storage behavior;
- `needs_judgment: true` whenever interpretation remains or anything is anomalous.

Record the summary with `research-manager record-summary RUN_ID SUMMARY.json --json`. Keep raw evidence intact at the referenced paths.

### 6. Interpret with Sol xhigh

Use `research-manager compare RUN_ID... --json` only to align measurements. It does not choose a winner. The main agent must inspect the raw evidence when:

- a result is near a threshold or contradicts the hypothesis;
- variance, missing data, instability, leakage, or failures could change the conclusion;
- aggregate and subgroup results disagree;
- the change affects the evaluation pipeline itself;
- a keep/revert decision would redirect substantial compute or research time.

Use `research-manager inspect RUN_ID --section evidence --json` for the complete evidence manifest, or `--section spec|record|summary|health|all` for lossless stored structured state. The ordinary handoff is only an index; omission counters mean “retrieve on demand,” never “discarded.”

Prefer matched comparisons. Preserve the evidence boundary between exploratory diagnostics, validation results, and held-out or official benchmark evidence. Seek confirmation on new seeds or the appropriate held-out evaluation before claiming a robust improvement.

### 7. Decide and checkpoint

After scientific interpretation, record one explicit decision:

```bash
research-manager decide RUN_ID \
  --decision keep|revert|refine|confirm|redirect|stop \
  --rationale "EVIDENCE-BASED RATIONALE" \
  --next "NEXT DECISION OR EXPERIMENT" --json
```

Update the cross-session strategy only when it materially changes:

```bash
research-manager checkpoint --strategy "..." --hypothesis "..." \
  --status "..." --next-decision "..." --json
```

Then either stop or begin a newly predeclared run. Do not silently mutate an old run to represent a new hypothesis.

## Spend context on research

- Batch deterministic reads and filters into one command when the next action does not require intermediate judgment.
- Prefer compact JSON, deltas, targeted searches, and metric reducers over full logs or raw tables.
- Avoid repeating plans, unchanged state, or earlier explanations in every update.
- Keep durable facts in `.research/`; keep transient operational chatter out of the scientific handoff.
- Bounded output limits model-facing context only. Never delete, truncate, rewrite, or replace raw logs, metrics, checkpoints, plots, or artifacts because of a context budget.
- Start a new conversation from `research-manager handoff --json` when the old context is mostly operational history.
- Escalate uncertainty to the main agent; never compress away evidence merely to meet a token target.

Token efficiency is acceptable only when the scientific conclusion, caveats, required evidence, and next decision remain intact.
