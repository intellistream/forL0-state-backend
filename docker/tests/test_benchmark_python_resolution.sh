#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$REPO_ROOT/docker/lib/benchmark_python.sh"

TEST_TMP="$(mktemp -d /tmp/forl0-python-resolution.XXXXXX)"
cleanup() {
    rm -rf "$TEST_TMP"
}
trap cleanup EXIT

make_fake_python() {
    local path="$1"
    local status="$2"
    mkdir -p "$(dirname "$path")"
    printf '#!/usr/bin/env bash\nexit %s\n' "$status" > "$path"
    chmod +x "$path"
}

repository_root="$TEST_TMP/repository"
installed_root="$TEST_TMP/forl0-runtime"
make_fake_python "$repository_root/.venv-benchmark-cp39/bin/python" 1
make_fake_python "$installed_root/.venv-benchmark-cp310/bin/python" 0

# Regression for campaign 20260816_111556: ignore the incomplete repository
# venv and reuse the dependency-complete project venv from the installed runtime.
FORL0_BENCHMARK_PYTHON_ROOT="$installed_root"
selected="$(forl0_find_ready_benchmark_python "$repository_root")"
[[ "$selected" == "$(readlink -f "$installed_root/.venv-benchmark-cp310/bin/python")" ]]

# Verify the runner's bootstrap function returns before touching the incomplete
# wheel directory once the installed-runtime venv has passed import validation.
awk '/^bootstrap_benchmark_python\(\)/,/^}/' "$REPO_ROOT/docker/run_all_apps.sh" \
    > "$TEST_TMP/bootstrap_benchmark_python.sh"
source "$TEST_TMP/bootstrap_benchmark_python.sh"
REPO_ROOT="$repository_root"
BENCH_PYTHON=""
bootstrap_benchmark_python
[[ "$BENCH_PYTHON" == "$(readlink -f "$installed_root/.venv-benchmark-cp310/bin/python")" ]]

# An explicit interpreter is authoritative and must fail closed when incomplete.
FORL0_BENCHMARK_PYTHON_BIN="$repository_root/.venv-benchmark-cp39/bin/python"
if forl0_find_ready_benchmark_python "$repository_root" >/dev/null 2>&1; then
    echo "incomplete explicit benchmark Python was accepted" >&2
    exit 1
else
    status=$?
fi
[[ "$status" -eq 2 ]]
unset FORL0_BENCHMARK_PYTHON_BIN

# A ready repository-local venv takes precedence over the installed fallback.
make_fake_python "$repository_root/.venv-benchmark-cp39/bin/python" 0
selected="$(forl0_find_ready_benchmark_python "$repository_root")"
[[ "$selected" == "$(readlink -f "$repository_root/.venv-benchmark-cp39/bin/python")" ]]

echo "benchmark Python resolution tests passed"
