#!/usr/bin/env python3
"""清理 benchmark 结果目录"""

import shutil
from pathlib import Path

BENCHMARK_DIR = Path(__file__).parent.parent
RESULTS_DIR = BENCHMARK_DIR / "results"


def clean_results(confirm: bool = True) -> None:
    """清理 results 目录下的所有文件"""
    
    dirs_to_clean = [
        RESULTS_DIR / "raw",
        RESULTS_DIR / "figures", 
        RESULTS_DIR / "reports",
        RESULTS_DIR / "latency",
        RESULTS_DIR / "l0metrics",
        RESULTS_DIR / "profiles",
    ]
    
    # 查找 nexmark_* 目录
    nexmark_dirs = list(RESULTS_DIR.glob("nexmark_*"))
    
    # 统计文件数量
    total_files = 0
    for d in dirs_to_clean:
        if d.exists():
            total_files += len(list(d.glob("*")))
    total_files += len(nexmark_dirs)
    
    if total_files == 0:
        print("✓ Results 目录已经是空的")
        return
    
    print(f"将清理以下目录中的 {total_files} 个文件/目录:")
    for d in dirs_to_clean:
        if d.exists():
            files = list(d.glob("*"))
            if files:
                print(f"  - {d.relative_to(BENCHMARK_DIR)}: {len(files)} 个文件")
    if nexmark_dirs:
        print(f"  - nexmark_* 目录: {len(nexmark_dirs)} 个")
    
    if confirm:
        response = input("\n确认清理? [y/N]: ").strip().lower()
        if response != 'y':
            print("已取消")
            return
    
    # 执行清理
    for d in dirs_to_clean:
        if d.exists():
            for f in d.glob("*"):
                if f.is_file():
                    f.unlink()
                elif f.is_dir():
                    shutil.rmtree(f)
    
    # 清理 nexmark_* 目录
    for d in nexmark_dirs:
        shutil.rmtree(d)
    
    print("✓ 清理完成")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="清理 benchmark 结果目录")
    parser.add_argument("-y", "--yes", action="store_true", help="跳过确认直接清理")
    args = parser.parse_args()
    
    clean_results(confirm=not args.yes)


if __name__ == "__main__":
    main()
