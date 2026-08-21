# Complete research-loop source snapshot

This standalone folder contains the complete source-level research-loop implementation copied outside the `RL` repository on 2026-08-21.

Included:

- `scripts/research_manager.py`
- `scripts/cluster_manager.py`
- `scripts/benchmark_monitor_token_savings.py`
- `scripts/muse_research_worker.py`
- `scripts/install_codex_research_loop.py`
- all associated tests under `tests/`
- the full installable package under `codex-research-loop/`
- the Muse-wrapper correction contract under `review_contracts/`

Excluded intentionally:

- scientific experiment implementations and datasets
- `.research` run state and evidence
- training logs, checkpoints, and artifacts
- credentials and user-wide installed files

The Git repository in this directory is local, has no remote, and exists only to make subsequent review diffs easy to inspect.
