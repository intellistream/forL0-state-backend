# SDK compatibility correction, 2026-11-18

Main's benchmark/scripts/collect_profile.py uses minimum-l0-tuner-mb=64, independently of the actual l0_mem_alloc request. The original offline package incorrectly passed the per-TM allocation quota as the tuner management limit. The correction restores this separation through FORL0_L0_TUNER_CAPACITY_BYTES without increasing the actual allocation request.

- Existing native suite: 148 tests passed on the ARM64 build host.
- Actual disconnected root-launcher check on the bare host: WC, 4096 keys, one repeat, warmup2s/measure5s, explicit DRAM validation, shared2MiB arm. Status complete, one passed job, independent oracle matched.
- Both actual probe processes reported tuner_capacity_bytes=67108864, requested_bytes=1048576, capacity_bytes=1048512; no full64MiB data region was allocated by the DRAM test allocator.
- The new runtime was unpacked from unchanged base parts000–007 plus part008; every final runtime file passed the updated MANIFEST checksum. Job SHA remained 7f6929397ca2187d14b26ceb52ce1dd1a7ab7e276738b9606e45a65f3d337c15.
- Read-only status displayed the completed result directly in the terminal. Final UI shows recorded SDK errors by default with --status; no additional detail flag or log export is required. No full performance matrix was run and no fake SDK test was added.

The bare host has no vendor L0 hardware. These checks validate packaging, quota separation and the real Flink job using the explicitly labelled DRAM allocator. They do not establish the partner SDK's internal reason for its earlier init failure or prove real L0 throughput.
