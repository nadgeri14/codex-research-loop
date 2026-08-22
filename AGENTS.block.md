# Token-efficient research loop — native Muse implementor

- Apply this workflow only to long-running scientific/ML research and experiment series. Resume from `.research/state.json` with `research-manager handoff --json`; never reconstruct from transcripts.
- GPT-5.6-Sol at `xhigh` is the main scientific brain: diagnosis, hypotheses, design, contracts, evidence interpretation, and go/no-go. GPT-5.6-Sol at `high` is the read-only implementation reviewer.
- Delegate every execution-ready code/config/test change to the native `muse_implementor` custom agent, pinned to `meta/muse-spark-1.2-contributor` at `xhigh`. Give it a bounded contract, exact ownership, approved checks, and no inherited conversation history. One write-capable implementor at a time.
- Sol never implements research changes; Muse never makes scientific decisions. Every Muse diff and validation handoff receives Sol-high read-only review before a consequential launch or conclusion. If Muse is unavailable, stop without substitution.
- Never use an LLM to wait for a deterministic external condition. Persist minimal state, arm exact wake conditions, hand the blocking monitor command to `cluster_monitor`, and end the frontier continuation.
- Route evidence deterministically first, Luna-low only for bounded ambiguous material events, and Sol only for scientific reasoning. Ordinary progress or elapsed time produces no assistant turn.
- Preserve raw evidence, provenance, hashes, independent replay, frozen evaluations, paired uncertainty, reproducibility, and auditability. Context caps limit model input, never stored evidence.
- Prefer compact handoffs and incremental reads. Never repeatedly reread unchanged logs, skills, handoffs, or large context merely to check whether a job finished.
