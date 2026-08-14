# Plans

Working design/implementation plans for in-progress Phoenix work (HA, replication, indexing, etc.).

**Why this dir exists:** plans authored in-repo are git-tracked, so they get version history and
are immune to the `~/.claude/plans/` retention sweep (which prunes that global scratch dir by mtime,
30-day default). Plans you want to keep belong here, not in `~/.claude/plans/`.

**Convention:** new plans for this repo are written here by default. Reference/analysis docs
(baselines, post-mortems, design spectrums) continue to live in `docs/`.
