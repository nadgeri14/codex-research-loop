# Standalone research-loop source workspace

- This is a clean, standalone source snapshot for reviewing and correcting the complete Codex research-loop implementation.
- Make all changes inside this directory only.
- Do not edit, install into, or copy changes back to `/lustre/nvwulf/home/anadgeri/RL`, `~/.codex`, or `~/.agents` unless the user later requests a separate reviewed installation step.
- Do not invoke Muse or any other implementation model automatically. The user wants this folder available for external review.
- Preserve cluster storage safety: keep caches and large temporary artifacts under `/lustre/nvwulf/scratch/anadgeri/codex-cache`, never `/tmp` or `/dev/shm`.
- Do not copy credentials or experiment data into this workspace.
- Research run state, experiment outputs, checkpoints, logs, and `.research/state.json` are intentionally excluded; this folder contains source code and source-level tests only.
