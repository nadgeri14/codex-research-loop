---
name: research-loop
description: Use for long-running scientific or ML research, autoresearch, iterative training/evaluation, benchmark optimization, hypothesis testing, training-health diagnosis, cluster experiments, or resuming such work across conversations. Provides an evidence-preserving, token-efficient loop with durable experiment state and silent-failure detection. Do not use for ordinary one-off coding, debugging, or a single short test unless it belongs to an experiment series.
---

# Research Loop

Preserve research quality by keeping scientific judgment with the selected main model and moving only deterministic bookkeeping, scheduler observation, and result alignment into scripts. Treat compact summaries as indexes to raw evidence, never as replacements for it.

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

## Keep the quality boundary

The main research agent owns all work that benefits from frontier reasoning:

- scientific diagnosis and literature-grounded understanding;
- hypothesis formation and falsification criteria;
- baseline, treatment, metric, dataset, seed, and evaluation design;
- consequential research code or configuration changes;
- interpretation of results and anomalies;
- keep, revert, refine, confirm, redirect, and stop decisions.

Do not lower the user's main model or reasoning effort for those steps. Do not delegate scientific judgment to the cluster monitor, `research-manager`, `cluster-manager`, or a cheaper model.

Use deterministic tools for mechanical work:

- `research-manager` validates schemas, records state, synchronizes job states, aligns metrics, and emits bounded handoffs;
- `research-manager health RUN_ID --json` scans recorded logs for non-finite values, instability, stalled progress, throughput collapse, distributed/data/checkpoint failures, and other bounded health signals;
- `cluster-manager` reads Slurm, resource, GPU, and bounded log deltas, returning control when common training failures appear;
- the configured `cluster_monitor` may wait for a genuine long-running job, but it receives only job IDs, optional log paths, and a stop condition.

Use scripts directly when they suffice. Do not create an agent merely to run a command or poll state.

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

Use the main agent to inspect and change the scientific code. Run focused unit, integration, and smoke checks appropriate to the risk. Record the checks actually completed:

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

### 4. Wait outside the reasoning loop

For a one-off check, use `research-manager sync RUN_ID --json` or one compact `cluster-manager status` call. For a genuine wait, use one `cluster_monitor` with `cluster-manager watch`; resume the main research agent only for a state change, milestone, anomaly, completion, user request, or scientific decision.

Never poll unchanged state in the main thread. Never feed routine elapsed-time updates or duplicate log tails back into the scientific context.

Run `research-manager health RUN_ID --json` after the first meaningful training progress, on a reported anomaly, at completion, and before interpreting results. A warning or critical signal must return control to the main agent. Treat health checks as triage, not proof: inspect the cited raw log and relevant code before diagnosing, canceling, or changing the experiment.

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

### 6. Interpret with frontier reasoning

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
