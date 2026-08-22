# Native Muse implementor smoke test

This verifies the complete Codex-native path without touching a research
repository:

```text
Sol main thread
  -> native custom-agent spawn
  -> codex-router
  -> meta/muse-spark-1.2-contributor at xhigh
  -> Codex edit and validation tools
  -> bounded result returned to Sol
```

## Preconditions

1. Install this package and start a fresh Codex session.
2. Confirm that `muse_implementor` is visible as a custom agent.
3. Confirm that codex-router is healthy and that
   `meta/muse-spark-1.2-contributor` is present in the model catalog.
4. Configure the Meta credential through codex-router. Never copy a credential
   into this repository or into a prompt.
5. Choose a persistent scratch directory. Do not use `/tmp` or `/dev/shm` on a
   RAM-constrained cluster.

## Prompt

Replace `/persistent/scratch/native-muse-agent-smoke` with an allowed scratch
path, then send this prompt to a Sol Codex thread:

```text
Spawn the native muse_implementor custom agent with no inherited conversation
history. It owns exactly this file:
/persistent/scratch/native-muse-agent-smoke/muse_implementor_ok.txt

It is not alone in the filesystem and must not inspect, modify, delete, or
revert any other path. Create the owned file with exactly the bytes
MUSE_IMPLEMENTOR_OK followed by one newline. Validate it with wc -c and
sha256sum, report the path/count/hash, and make no scientific decisions. Wait
for the result, then close the completed child thread.
```

## Expected evidence

The artifact must be exactly 20 bytes:

```text
MUSE_IMPLEMENTOR_OK\n
```

Its SHA-256 must be:

```text
e423473c6b5949d187d634e8d8dd9b8127965095b1e0da9fd71af4f35d59fcc4
```

The router timing log should contain at least one successful entry naming:

```text
model=meta/muse-spark-1.2-contributor provider=meta status=200
```

No file in the research repository should change.

## Known compatibility failure

If Meta rejects the spawn before Muse runs with an error resembling
`input[n] did not match any supported type`, the router is forwarding Codex's
internal `agent_message` envelope. Apply the version-appropriate fix in
`patches/codex-router-meta-agent-message.patch`, run the router tests, restart
the router, and retry the smoke once.

Do not work around this failure by restoring the obsolete direct Meta API
worker or raw `muse exec` lane. Those paths bypass the native custom-agent
contract and are intentionally absent from this repository.
