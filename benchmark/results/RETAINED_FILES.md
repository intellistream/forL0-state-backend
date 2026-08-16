# Retained benchmark evidence

Only `benchmark/results/latest/` is retained as completed benchmark evidence.
It currently contains campaign `20260811_170156` in a completely flat,
web-upload-friendly layout. The newest dedicated hardware calibration may also
be retained under `benchmark/results/profiles/`; failed timestamped benchmark
runs under `benchmark/results/runs/` are temporary diagnostics, not retained
evidence.

The exact source-path mapping is recorded in `latest/UPLOAD_MANIFEST.tsv`; run
identity and status are recorded in `latest/run_manifest.json`.

The retained campaign contains:

- one matched WordCount backend pair;
- four matched Client Usecase scenario pairs;
- six matched NexMark query pairs;
- the isolated workload plan;
- the distilled analysis of valid, superseded, and failed runs.

The rejected q18 TPS pair and incomplete `20260811_184222` campaign are
summarized in `latest/formal__reports__latest_analysis.md`. Their raw files are
not retained because they cannot support a matched performance comparison.
