# Benchmark results

`reproduce-all` uses two lifecycle locations:

- `runs/<run_id>/` is the isolated staging area while a campaign is running or
  after its latest attempt fails.
- `latest/` is the only completed campaign retained for publication.

Dedicated hardware calibration uses `profiles/profile_<timestamp>/`. Keep the
newest profile while it is being analyzed or replaced; it is independent of
the failed benchmark staging directories under `runs/`.

`latest/` is deliberately flat: it contains files only, with no subdirectories
or symlinks. Nested staging paths are encoded with `__`, for example:

```text
formal/raw/example.json -> formal__raw__example.json
formal/nexmark_123/nexmark_results.json
  -> formal__nexmark_123__nexmark_results.json
```

`latest/UPLOAD_MANIFEST.tsv` maps every flat filename to its original campaign
path. Select every file in `latest/` when uploading through the GitHub web UI.

A successful campaign atomically replaces the previous `latest/` and removes
its staging directory. A failed campaign does not replace `latest`; it remains
as the sole directory under `runs/` for diagnosis until the next attempt.

The current `latest/` contains the migrated usable campaign
`20260811_170156`. Superseded and invalid raw evidence was removed after its
important conclusions were distilled into
`latest/formal__reports__latest_analysis.md`.
