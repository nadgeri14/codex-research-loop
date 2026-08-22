# Native Meta Muse Spark implementation subagent

The research loop uses a native Codex custom agent named `muse_implementor` as
its sole implementation worker. The profile pins
`meta/muse-spark-1.2-contributor` at `xhigh` and retains the Codex harness for
repository inspection, bounded edits, and validation. GPT-5.6-Sol at `xhigh`
remains the scientific lead; GPT-5.6-Sol at `high` remains the read-only code
reviewer. Luna-low and deterministic tools remain operational only.

Muse never owns research planning, experiment design, evidence interpretation,
scientific anomaly diagnosis, or launch/go-no-go decisions.

## Availability and session loading

The installed profile is `~/.codex/agents/muse-implementor.toml`. Codex loads
custom agent profiles and model catalogs when a session starts, so fully restart
Codex after installation or profile/catalog changes. The local router must be
healthy, the Meta provider must be authenticated, and the exact namespaced model
must exist in the routed catalog.

Do not change the main thread's model to Muse. The main research thread and
`research_lead` stay on GPT-5.6-Sol at `xhigh`; only the delegated
`muse_implementor` thread uses Muse.

## Bounded handoff

Sol-xhigh creates an execution-ready contract containing only:

1. the intended behavior and non-goals;
2. exact owned paths and optional bounded read-only context paths;
3. preserved interfaces and scientific invariants;
4. acceptance criteria and caller-approved validation commands;
5. a contract ID and any correction findings from the reviewer.

Spawn `muse_implementor` with no inherited conversation history. Do not send the
full research transcript, complete historical logs, the entire repository, or
unchanged handoffs. One write-capable Muse agent owns a set of paths at a time;
never assign overlapping writes concurrently.

## Implementor boundary

The custom profile requires Muse to:

- inspect only the declared owned/context paths;
- edit only owned paths and preserve unrelated user or agent work;
- run only validation commands explicitly included in the handoff;
- avoid cluster submission, cancellation, monitoring, credential changes,
  router changes, evidence mutation, and subagent spawning;
- stop and return an exact question when scope, evidence, or scientific intent
  is ambiguous;
- return a compact handoff with changed paths, checks/results, unresolved
  questions, and evidence references.

The profile is an instruction boundary, not a substitute for review. The
Sol-high `research_code_reviewer` must inspect the actual diff and validation
evidence against the contract before any consequential launch or conclusion.

## Review and correction

1. Muse implements and returns the bounded handoff.
2. Sol-high reviews read-only, findings first, with exact file/line references.
3. An implementation defect becomes a bounded correction task for a fresh Muse
   turn with the same ownership boundary.
4. A scientific ambiguity returns to Sol-xhigh, which revises only the contract
   or scientific decision.
5. A favorable review returns a bounded verdict to Sol-xhigh; only Sol-xhigh
   owns launch, interpretation, and go/no-go.

Neither Sol agent patches the implementation. Muse does not review its own work
or interpret scientific results.

## Failure behavior

If the native profile, router, or Muse model is unavailable, stop and report the
failure. Do not silently substitute Sol, Luna, another provider/model, or a
generic worker. Do not keep retrying unchanged failures and never use an LLM to
wait for router, scheduler, process, filesystem, or training state.

## Expected control flow

```text
Sol-xhigh research lead
        |
        v
bounded implementation contract
        |
        v
native muse_implementor (Muse Spark xhigh)
        |
        v
diff + bounded validation handoff
        |
        v
Sol-high read-only reviewer
        |
        +-- correction ----------> fresh bounded Muse turn
        +-- scientific ambiguity -> Sol-xhigh contract decision
        `-- favorable -----------> Sol-xhigh launch/go-no-go
```
