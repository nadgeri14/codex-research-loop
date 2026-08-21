# Meta Muse Spark implementation worker — sole implementation worker (direct API, Phase 1)

Meta Muse Spark is the only model allowed to implement research code, configuration, or test changes. GPT-5.6-Sol at `xhigh` owns all research reasoning and creates the implementation contract. GPT-5.6-Sol at `high` read-only reviews Muse diffs/validation. Muse Spark executes only bounded, execution-ready tasks via the direct API worker. Luna at `low` remains operational only.

Muse Spark does not own research planning, experiment design, evidence interpretation, go/no-go, or review.

## Availability

The wrapper uses the Meta Responses endpoint directly — no `muse` CLI is invoked. Production endpoint is `https://api.meta.ai/v1/responses` (provider `meta`, model `muse-spark-1.2-contributor`, reasoning `xhigh`). Credentials are read from the existing Muse credential JSON (`providers.meta.api_key` or `providers.meta.access_token`) without printing. Tests may enable `MUSE_SPARK_TEST_ENDPOINT` (loopback-only, name contains `TEST`) and `MUSE_SPARK_TEST_CREDENTIAL_PATH` (path contains `TEST`). If credentials or endpoint are unavailable, stop without substitution.

## Handoff contract

Sol-xhigh creates the implementation contract and declares exact owned paths, optional read-only context paths, and finite caller-owned validation `check-json` argv arrays. The wrapper reads those files into a bounded prompt with SHA-256 identities, caps input/output/wall budgets, and calls the API once with no tools.

## Invocation (operational path)

```bash
python scripts/muse_research_worker.py \
  --root REPO --contract CONTRACT \
  --owned-path REL --context-path REL \
  --check-json '["python","-m","py_compile","scripts/muse_research_worker.py"]' \
  --scratch-root /lustre/nvwulf/scratch/anadgeri/codex-cache \
  --wall-seconds 1800 --http-seconds 60 \
  --max-input-bytes 204800 --max-output-tokens 8000 --max-response-bytes 2097152 \
  --attempt-kind initial --contract-id ID
```

Do not use raw `muse exec`. Muse Spark remains the sole implementation model; the local CLI orchestration layer is bypassed. No model tools, no shell tools, no subagents. Finite budgets, canonical scratch under `/lustre/nvwulf/scratch/anadgeri/codex-cache` (no `/tmp`/`/dev/shm`/tmpfs/ramfs, no chmod of shared root), wrapper-owned `0700`/`0600` evidence and env dirs (`TMPDIR` etc. redirected). The model never chooses validation; it proposes only `replace`/`create`/`delete` edits within owned scope.

## Request/response and edits

Request: `POST /v1/responses` with exact model, `reasoning.effort xhigh`, `stream false`, finite `max_output_tokens`, no `tools`, bounded prompt with contract + owned SHAs + missing markers + context. Response must be HTTP 2xx, bounded bytes, `status completed`, exact model, no tool calls, exactly one `output_text` containing strict JSON with `schema_version`, bounded `changes`, nonempty `handoff`, and informational `claimed_checks`. Reject `incomplete`, fences, prose, duplicate payloads, unknown fields.

Edits are transactional: every file computed in memory, absolute/`..`/NUL/symlink/special/oversize/hash-mismatch/ambiguous/create-overwrite/duplicate/conflicting rejected, all-or-nothing via staging and atomic rename.

## Validation, scope, locking, evidence

Validations are caller-predeclared; the wrapper runs each in a new process group under scratch env, TERM→KILL with bounded grace, bounded raw capture, success only from true exit code (no pipelines). Scope verified via git baseline on `-z`/`diff`/`HEAD`/index movement; dirty outside-owned untracked/tracked changes, renames with spaces, deletions, Git errors fail closed. Repository/contract lock held from baseline through state persistence; concurrent wrapper refused. Attempts finite (initial/correction ceilings), unchanged-failure fingerprint suppressed, state locked/atomic/durable. Evidence includes contract hash, file identities, sanitized request hash, raw response hash, HTTP meta without auth, parsed proposal, applied paths, validation argv/results, baseline/scope, attempt kind, exit classification, bounded handoff — never credentials.

No user-wide installation. Installer unchanged in Phase 1.

## Review and correction loop

Same as SKILL.md: Sol-high reviews diff/evidence vs. contract; favorable → bounded verdict to Sol-xhigh; unfavorable → correction via wrapper with `attempt-kind correction`; ambiguity → Sol-xhigh revises contract. Never substitute another model.
