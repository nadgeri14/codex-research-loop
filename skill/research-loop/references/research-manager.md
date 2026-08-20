# Research manager reference

Use the globally installed executable on `PATH`:

```text
research-manager
```

The command defaults to the nearest Git root. Put `--root REPO` before the subcommand when working elsewhere. It writes only under `REPO/.research/` and never submits, cancels, or requeues a cluster job.

Run manager commands through a non-login shell when available so cluster module startup messages do not contaminate JSON output.

## Durable layout

```text
.research/
  state.json                 compact current objective and next decision
  runs.jsonl                 append-only event ledger
  decisions.jsonl            append-only scientific decisions
  health/RUN_ID.json         bounded training-health index and progress snapshot
  monitors/RUN_ID/
    spec.json                exact wake conditions and configurable invariants
    state.json               atomic cursors, progress state, dedupe ledger, telemetry
    events.jsonl             compact structured event ledger
    wake.json                latest material Sol wake packet, when present
  runs/RUN_ID/
    spec.json                predeclared experiment
    record.json              current operational state
    summary.json             bounded result index, when available
```

Raw metrics, logs, checkpoints, plots, and artifacts stay at their original paths. `summary.json` records those paths; it does not ingest the files.

## Lifecycle

```text
PLANNED -> VALIDATED -> QUEUED -> RUNNING -> REDUCING -> READY -> DECIDED
    |           |          |          |           |
    +-----------+----------+----------+-----------+-> FAILED/CANCELLED -> DECIDED
```

The manager enforces legal transitions. A refined hypothesis should receive a new run ID.

## Commands

```bash
research-manager init --objective "OBJECTIVE" --json
research-manager template spec
research-manager plan SPEC.json --json
research-manager validate RUN_ID --check "CHECK" --evidence PATH --json
research-manager record-launch RUN_ID --job-id JOB_ID --log PATH --artifact PATH --json
research-manager arm-monitor RUN_ID --phase TRAINING --target-step STEP \
  --checkpoint PATH --next-scientific-action "ACTION AFTER WAKE" --json
research-manager sync [RUN_ID ...] --json
research-manager health RUN_ID --json
research-manager transition RUN_ID RUNNING|REDUCING|FAILED|CANCELLED --reason "..." --json
research-manager template summary
research-manager record-summary RUN_ID SUMMARY.json --json
research-manager compare RUN_ID [RUN_ID ...] --json
research-manager inspect RUN_ID --section all|spec|record|summary|health|monitor|evidence --json
research-manager decide RUN_ID --decision DECISION --rationale "..." --next "..." --json
research-manager checkpoint --strategy "..." --hypothesis "..." --status "..." --next-decision "..." --json
research-manager status --json
research-manager handoff --limit 5 --json
research-manager doctor --json
```

Decisions are `keep`, `revert`, `refine`, `confirm`, `redirect`, or `stop`.

`sync` makes one compact, read-only call to the shared `cluster-manager`. Use it after a meaningful scheduler transition, not as a conversational polling loop.

`arm-monitor` is the handoff boundary for a long-running operation. It writes an idempotent monitor specification and restart-safe state, records exact wake conditions, and returns a `monitor_command`. Run that command once through the narrow `cluster_monitor`, then end the frontier continuation. The command blocks with `--until wake --timeout 0`; elapsed time and ordinary progress never return control to an LLM.

Invariant and routing thresholds are explicit in `spec.json`. Override defaults at arm time with repeatable `--threshold NAME=VALUE`; supported names are `scheduler_unknown_seconds`, `dedupe_window_seconds`, `metric_window`, `minimum_metric_samples`, `consecutive_violations`, `loss_mad_z`, `gradient_mad_z`, `throughput_ratio`, `step_regression_tolerance`, and `luna_min_confidence`. Step and log stall windows also have the named `--stall-seconds` and `--log-stall-seconds` flags.

The event monitor reads only bytes appended after each log cursor. Each cursor records path, inode, byte offset, modification time, last observed step, and an unfinished final-line fragment. Rotation, truncation, job restart, duplicate lines, and interrupted atomic state writes are handled without rereading the whole log. Expected progress is state only; material events use the explicit taxonomy `MILESTONE`, `CHECKPOINT`, `EVAL_COMPLETED`, `TRAINING_COMPLETED`, `KNOWN_WARNING`, `UNKNOWN_WARNING`, `INVARIANT_FAILED`, `PROCESS_FAILED`, `STALL`, `ARTIFACT_MISSING`, `ARTIFACT_INVALID`, and `SCIENTIFIC_REVIEW_REQUIRED`.

Routing is centralized and deterministic. Routine progress is recorded and monitoring continues. Known operational issues follow their configured deterministic route. A bounded, deduplicated unknown event may return `route: "LUNA"` with a strict response schema; validate the answer using `cluster-manager resolve-luna`. A routine classification returns to monitoring, while an invalid, low-confidence, `FRONTIER_REQUIRED`, or scientifically significant result emits one compact Sol wake packet. Luna never chooses a scientific action.

Monitor telemetry includes deterministic poll count, incrementally read bytes, emitted and deduplicated events, Luna calls and token counts when supplied, Sol wakeups, no-change frontier wakeups, whole-log reads, and handoff bytes. The invariants are `frontier_no_change_wakeups == 0` and `full_log_reads == 0` during monitoring.

`health` scans up to the last 4 MiB of every recorded log by default. It detects explicit exceptions and training failures, parses steps/loss/gradient norms/throughput, warns about stalled progress and relative spikes or collapse, and stores a small progress snapshot for the next check. Use `--tail-bytes` up to 16 MiB or change `--stale-seconds` when a workload has unusually sparse logging. Warnings and critical signals require main-agent review; the command never cancels a run or makes a scientific decision.

`inspect` bypasses conversational reductions for stored structured state. `--section monitor` returns the monitor spec, compact state, and current wake packet without reading the event ledger or raw log. `--section evidence` returns every registered evidence path with existence, size, and modification time without reading file contents. `--section all` returns the complete stored spec, record, summary, health report, monitor index, and evidence manifest. Read raw files directly when those structures point to evidence needed for a diagnosis.

## Experiment spec

Generate the current template with `research-manager template spec`. Required fields are:

```json
{
  "run_id": "exp-001",
  "hypothesis": "A precise falsifiable statement.",
  "change": "The smallest coherent intervention.",
  "baseline": {"configuration": "control"},
  "treatment": {"configuration": "treatment"},
  "primary_metric": {"name": "validation_score", "direction": "maximize"},
  "success_criteria": {"minimum_delta": 0.01},
  "failure_criteria": {"maximum_regression": -0.01},
  "evaluation": {"benchmark": "validation", "seeds": [1, 2, 3]}
}
```

The schema permits domain-specific fields such as secondary metrics, resources, budgets, tags, notes, and evidence paths. Keep structured inputs below 1 MB and reference bulky material by path.

## Result summary

Generate the current template with `research-manager template summary`. Required fields are:

```json
{
  "status": "complete",
  "primary_metrics": {"validation_score": {"value": 0.72, "std": 0.01}},
  "baseline_delta": {"validation_score": 0.02},
  "sample_counts": {"examples": 1000, "seeds": 3},
  "runtime": {"wall_seconds": 1200, "gpu_hours": 0.33, "peak_memory_gib": 42.0},
  "failure_modes": [],
  "anomalies": [],
  "health_checks": {
    "nonfinite": "pass",
    "step_progress": "pass",
    "throughput": "pass",
    "gradient_norm": "pass",
    "distributed": "pass",
    "data_pipeline": "pass",
    "checkpoint_and_storage": "pass"
  },
  "evidence_paths": ["results/full-metrics.json", "logs/run.log"],
  "needs_judgment": true
}
```

`status` is `complete`, `failed`, or `anomaly`. Completed summaries require at least one primary metric and non-empty sample counts. Every summary requires at least one evidence path. Anomalies force `needs_judgment: true`. A stored training-health warning or critical result cannot be hidden by recording a summary with `needs_judgment: false`.

## Bounded output behavior

- `handoff` is capped at 32 KiB and includes at most ten active runs plus a bounded recent history.
- `status` is capped at 32 KiB and reports the active-run count when details are omitted.
- JSON ledgers are tailed from a bounded byte window rather than loaded wholesale.
- `compare` accepts at most twenty runs, caps output at 64 KiB, and aligns numeric values without interpreting them.
- `health` is capped at 64 KiB and retains every raw log untouched.
- event-monitor Luna packets contain only compact state, one exact new event, a bounded evidence window, and the response contract;
- `doctor` validates schemas and reports a bounded set of missing local evidence paths without opening evidence contents.

These are context budgets, not storage limits. Omitted fields are counted, all stored structured state is available through `inspect`, and all raw evidence remains at its registered paths. If a bounded result is insufficient for a scientific decision, retrieve the relevant section and inspect the raw evidence directly.
