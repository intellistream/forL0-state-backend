#!/usr/bin/env bash
# Canonical isolated correctness-gate matrix.

forl0_smoke_plan() {
    cat <<'EOF'
S01|client_usecase|contract_baseline||hashmap|Client contract path
S02|client_usecase|contract_baseline||forl0|Client ForL0 path
S03|nexmark|forl0_tps_probe|q18|hashmap|NexMark Java/runtime baseline
S04|nexmark|forl0_tps_probe|q18|forl0|NexMark ForL0 runtime path
EOF
}
