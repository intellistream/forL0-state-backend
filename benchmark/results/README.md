# Historical benchmark evidence

This directory preserves the raw and derived outputs from the June–July 2026
ForL0 benchmark campaign. The archive includes exploratory NexMark runs,
WordCount and client-use-case measurements, hardware-monitor samples, run logs,
and the figures generated from those records.

These files are historical backend-path evidence. They are not a matched
real-L0 evaluation: the archived runs do not provide the device and runtime
identity required by `research_paper/evidence_index.json`. In particular, a
ForL0 backend label, JNI activity, or execution on an Ascend host must not be
interpreted as proof that L0 hardware was active.

Quantitative claims should cite a specific raw file, code commit, environment
record, and matched experiment grid. New paper claims remain subject to
`research_paper/validate_evidence_index.py`; this archive does not relax that
gate. Repeated and negative runs are retained because they record stability
limits and failed configurations rather than only favorable outcomes.

## Latest campaign analysis

The curated analysis for the 2026-08-11 W01–W02, N01–N14, C01–C08 campaign is
[reports/latest_analysis.md](reports/latest_analysis.md). It explicitly excludes
the twelve NexMark runs that failed before SQL submission and documents the
source JSON for every retained comparison.

The HTML report generated at 2026-08-11 15:22 is legacy-unscoped: it mixes the
latest q3 result with historical NexMark queries and mislabels the latest
scalar-state Client result as `contract_baseline`. Preserve it for audit, but do
not use it as the campaign-specific analysis. A post-fix run carrying one
`run_id` should regenerate the scoped HTML report.
