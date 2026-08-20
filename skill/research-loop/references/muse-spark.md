# Meta Muse Spark implementation worker — sole implementation worker

Meta Muse Spark is the only model allowed to implement research code, configuration, or test changes. GPT-5.6-Sol at `xhigh` owns all research reasoning, planning, diagnosis, interpretation, and decisions and creates the implementation contract. GPT-5.6-Sol at `high` read-only owns review of Muse diffs and validation reports against that contract. Muse Spark executes only bounded, execution-ready implementation tasks under Sol-xhigh's contract. Luna at `low` remains limited to operational cluster/job/process/log checks and event monitoring and must never implement research changes.

Muse Spark does not own research planning, experiment design, evidence interpretation, go/no-go decisions, or implementation review.

## Availability

Confirm the command and configured model without exposing credentials:

```bash
command -v muse
muse --version
```

The expected Meta model ID is `muse-spark-1.2-contributor` at `xhigh`. If that exact model is unavailable or the Muse CLI is not authenticated, stop and report the limitation to Sol instead of silently substituting Sol, Luna, Spark from another provider, or another model.

## Handoff contract

Sol-xhigh must create the implementation contract before invoking Muse. Give Muse a bounded prompt containing:

- the implementation objective and why it is needed;
- files or modules it owns;
- interfaces and behavior that must remain stable;
- explicit acceptance criteria and focused checks;
- a reminder that other agents may be editing the repository and their changes must not be reverted;
- a requirement to stop and report any scientific or experimental design ambiguity.

Muse is the sole implementation worker for research code, configuration, and test changes, including tiny changes. Do not route such work to Sol or Luna.

Place long prompts in a workspace file and use `--prompt-file` rather than complex shell quoting. Keep package caches and large temporary artifacts under the cluster scratch cache described by the repository instructions.

## Invocation

Run Muse headlessly from the research repository:

```bash
muse exec --provider meta --model muse-spark-1.2-contributor \
  --reasoning-effort xhigh --workspace REPO --trust-workspace \
  --user-input-auto-resolve --prompt-file PROMPT_PATH
```

Keep Muse's sandbox and approvals enabled. Do not add `--yolo`, `--disable-sandbox`, or `--disable-approval`. Use the existing shared worktree unless Sol deliberately assigns an isolated worktree and has a merge plan.

## Review and correction loop (Sol-high reviewer -> Sol-xhigh lead)

After Muse finishes, the read-only research_code_reviewer (GPT-5.6-Sol at `high`) must review every resulting diff and validation report against the Sol-xhigh implementation contract before a consequential launch or scientific conclusion. Muse's report is an implementation handoff, not scientific evidence. All code, configuration, and test review remains with the Sol-high reviewer; the Sol-xhigh research lead does not inspect raw code or implementation details.

- The reviewer is review-only: findings first, exact file/line references, correctness/regression/edge-case/test-coverage focus, no edits and no new scientific decisions. The reviewer may and must assess test and validation evidence for implementation correctness, but it must not interpret scientific experiment results or make scientific decisions.
- If the review is favorable, the reviewer produces a bounded review verdict/evidence handoff (with exact file/line references, how it aligns with the contract, residual risks, and validation evidence) to the Sol-xhigh research lead. The research lead owns launch/go-no-go and scientific interpretation without duplicating routine review and without inspecting raw code or implementation details.
- If the review is unfavorable, the reviewer returns a bounded correction request to Muse describing what must change and why, with exact file/line references; Muse then reimplements and the reviewer reviews again. Neither Sol-xhigh nor the reviewer patches the implementation itself.
- If the reviewer encounters a scientific ambiguity that requires a research judgment, it is returned to the Sol-xhigh research lead with a bounded, exact implementation-evidence handoff (with file/line references and the relevant behavior) so the lead can resolve only the scientific question or revise the contract without reviewing code; Muse then implements the revised contract and the reviewer reviews again.
- If Muse is unavailable or fails at any point, stop and report the limitation; do not substitute Sol, Luna, Spark from another provider, or another model.

When an implementation detail could affect a research decision, the reviewer must provide that bounded, exact implementation-evidence handoff (with file/line references and the relevant behavior) so the Sol-xhigh lead can decide without reviewing code.

Muse must not make scientific decisions, choose experiment design, or interpret research results. Any scientific ambiguity encountered during implementation must be returned to Sol.
