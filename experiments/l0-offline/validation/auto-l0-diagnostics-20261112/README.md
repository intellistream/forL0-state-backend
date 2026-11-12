# Automatic L0 diagnostics, 2026-11-12

Actual checks on jinyun@36.140.239.86 using the packaged runtime in a network namespace with only loopback. This host has no L0 device or SDK. No benchmark job was run and no L0 memory pool was initialized.

- `diagnostic-only.json`: actual root launcher with `--diagnose`; records hostname, uid/HOME, device and driver evidence, expanded search roots, raw search results, and separate device/SDK findings. Diagnostic took 2.966 seconds. Its `failed` status correctly means the required hardware environment is unavailable. The `/tmp` search recorded a transient runc directory disappearing; this raw find error is retained.
- `automatic-preflight.json`: normal root launcher with `--workloads wc --arms brisk_l0_2`; the new inspection was invoked automatically, the unavailable tier was skipped, and the JSON/status retained specific guidance. Counts: one planned, zero passed/failed jobs, one skipped. This check preceded the final metadata/search-root additions to the helper.
- The bundled Python successfully imported `ctypes` and `runpy`. Python AST, Bash syntax and Git whitespace checks passed.

These checks validate diagnosis and failure reporting. Actual vendor SDK loading, L0 allocation and hardware performance still require the partner's machine. No simulated SDK or hardware was used.
