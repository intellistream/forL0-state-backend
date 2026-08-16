#!/usr/bin/env python3
"""Safely clean generated benchmark results while retaining documentation."""

import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils.config import get_results_dir

RESULTS_DIR = get_results_dir('')


def _is_real_direct_child(path: Path) -> bool:
    """Return true only for a non-symlink entry directly below RESULTS_DIR."""
    return (
        path.parent.resolve() == RESULTS_DIR.resolve()
        and path.name not in {"", ".", ".."}
        and not path.is_symlink()
    )


def _remove_entry(path: Path) -> None:
    if not _is_real_direct_child(path):
        raise RuntimeError(f"拒绝清理不安全的结果路径: {path}")
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()


def clean_results(confirm: bool = True) -> None:
    """Remove generated campaigns and legacy outputs, but keep result docs."""

    legacy_names = ["raw", "figures", "reports", "latency", "profiles", "hardware", "run_logs"]
    targets = [RESULTS_DIR / "latest", RESULTS_DIR / "runs"]
    targets.extend(RESULTS_DIR / name for name in legacy_names)
    targets.extend(sorted(RESULTS_DIR.glob("nexmark_*")))
    targets = [path for path in targets if path.exists() or path.is_symlink()]

    if not targets:
        print("✓ Results 目录已经是空的")
        return

    print(f"将清理 {len(targets)} 个生成结果入口（README 等说明文件会保留）:")
    for path in targets:
        print(f"  - {path}")

    if confirm:
        response = input("\n确认清理? [y/N]: ").strip().lower()
        if response != 'y':
            print("已取消")
            return
    
    for path in targets:
        _remove_entry(path)

    # Keep the run staging root available for the next campaign. Empty
    # directories are not required in Git and will be recreated by launchers.
    (RESULTS_DIR / "runs").mkdir(parents=True, exist_ok=True)
    print("✓ 清理完成")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="清理 benchmark 结果目录")
    parser.add_argument("-y", "--yes", action="store_true", help="跳过确认直接清理")
    args = parser.parse_args()
    
    clean_results(confirm=not args.yes)


if __name__ == "__main__":
    main()
