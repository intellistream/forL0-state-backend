#!/usr/bin/env python3
"""Generate local-only figures and a PDF analysis from a benchmark campaign."""

from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import ListedColormap
from PIL import Image
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch, mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.platypus import (
    Image as ReportImage,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


HASHMAP_COLOR = "#4C78A8"
FORL0_COLOR = "#E69F00"
FAIL_COLOR = "#D55E00"
SUCCESS_COLOR = "#0072B2"
GRID_COLOR = "#D9D9D9"


@dataclass(frozen=True)
class CellSpec:
    pair: str
    family: str
    backend: str
    scenario: str
    query: str | None = None


MATRIX: tuple[CellSpec, ...] = (
    CellSpec("WordCount", "WordCount", "hashmap", "stateful_counter_p4_probe"),
    CellSpec("WordCount", "WordCount", "forl0", "stateful_counter_p4_probe"),
    CellSpec("q18 TPS", "NexMark", "hashmap", "forl0_tps_probe", "q18"),
    CellSpec("q18 TPS", "NexMark", "forl0", "forl0_tps_probe", "q18"),
    CellSpec("q18 deep", "NexMark", "hashmap", "forl0_no_full_gc_lateq_deep", "q18"),
    CellSpec("q18 deep", "NexMark", "forl0", "forl0_no_full_gc_lateq_deep", "q18"),
    CellSpec("q19 TPS", "NexMark", "hashmap", "forl0_tps_probe", "q19"),
    CellSpec("q19 TPS", "NexMark", "forl0", "forl0_tps_probe", "q19"),
    CellSpec("q20 deep", "NexMark", "hashmap", "forl0_no_full_gc_lateq_deep", "q20"),
    CellSpec("q20 deep", "NexMark", "forl0", "forl0_no_full_gc_lateq_deep", "q20"),
    CellSpec("q9 pressure", "NexMark", "hashmap", "forl0_no_full_gc_allq_pressure", "q9"),
    CellSpec("q9 pressure", "NexMark", "forl0", "forl0_no_full_gc_allq_pressure", "q9"),
    CellSpec("q4 pressure", "NexMark", "hashmap", "forl0_no_full_gc_pressure", "q4"),
    CellSpec("q4 pressure", "NexMark", "forl0", "forl0_no_full_gc_pressure", "q4"),
    CellSpec("q3 extra SQL", "NexMark", "hashmap", "forl0_no_full_gc_extra_sql", "q3"),
    CellSpec("q3 extra SQL", "NexMark", "forl0", "forl0_no_full_gc_extra_sql", "q3"),
    CellSpec("Client contract", "Client", "hashmap", "contract_baseline"),
    CellSpec("Client contract", "Client", "forl0", "contract_baseline"),
    CellSpec("Client optimized", "Client", "hashmap", "forl0_optimized"),
    CellSpec("Client optimized", "Client", "forl0", "forl0_optimized"),
    CellSpec("Client pressure", "Client", "hashmap", "state_pressure_300k"),
    CellSpec("Client pressure", "Client", "forl0", "state_pressure_300k"),
    CellSpec("Client scalar", "Client", "hashmap", "scalar_state_probe_2m_ops64_batch"),
    CellSpec("Client scalar", "Client", "forl0", "scalar_state_probe_2m_ops64_batch"),
)


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_metrics(formal: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(formal.glob("nexmark_*/nexmark_results.json")):
        data = read_json(path)
        scenario = data.get("scenario_name")
        for backend, block in data.get("results", {}).items():
            for query, metric in block.get("query_results", {}).items():
                rows.append({
                    "family": "NexMark",
                    "scenario": scenario,
                    "query": query,
                    "backend": backend,
                    "throughput": float(metric["throughput"]),
                    "throughput_per_core": float(metric["throughput_per_core"]),
                    "cpu_cores": float(metric["cpu"]),
                    "sample_count": 1,
                    "source": str(path),
                })
    for path in sorted((formal / "raw").glob("*.json")):
        data = read_json(path)
        benchmark = data.get("benchmark")
        if benchmark == "wordcount":
            family = "WordCount"
            scenario = data.get("scenario")
        elif benchmark == "client-usecase":
            family = "Client"
            test_name = data.get("_metadata", {}).get("test_name")
            if test_name:
                scenario = str(test_name).removeprefix("client_usecase_")
            else:
                suffix = f"_{data.get('backend')}_{data.get('_metadata', {}).get('timestamp')}"
                scenario = path.stem.removeprefix("client_usecase_").removesuffix(suffix)
        else:
            continue
        rows.append({
            "family": family,
            "scenario": scenario,
            "query": None,
            "backend": data.get("backend"),
            "throughput": float(data["throughput"]),
            "throughput_per_core": float(data["throughput_per_core"]),
            "cpu_cores": None,
            "sample_count": len(data.get("repeat_samples", [])) or 1,
            "repeat_samples": data.get("repeat_samples", []),
            "source": str(path),
        })
    return rows


def metric_index(rows: list[dict[str, Any]]) -> dict[tuple[str, str, str, str | None], dict[str, Any]]:
    return {
        (row["family"], row["scenario"], row["backend"], row.get("query")): row
        for row in rows
    }


def failure_reason(spec: CellSpec) -> str:
    if spec.backend == "hashmap" and spec.query in {"q18", "q20"} and spec.scenario.endswith("lateq_deep"):
        return "TaskManager lost under pressure"
    if spec.backend == "forl0" and spec.query == "q3":
        return "L0 allocation + table capacity"
    if spec.backend == "forl0":
        return "strict L0 allocation failed"
    return "no valid sample"


def matrix_rows(index: dict[tuple[str, str, str, str | None], dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for spec in MATRIX:
        key = (spec.family, spec.scenario, spec.backend, spec.query)
        success = key in index
        output.append({
            "pair": spec.pair,
            "family": spec.family,
            "backend": spec.backend,
            "scenario": spec.scenario,
            "query": spec.query or "",
            "status": "success" if success else "failed",
            "reason": "" if success else failure_reason(spec),
        })
    return output


def paired_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[tuple[str, str, str | None], dict[str, dict[str, Any]]] = {}
    for row in rows:
        key = (row["family"], row["scenario"], row.get("query"))
        by_key.setdefault(key, {})[row["backend"]] = row
    output = []
    for (family, scenario, query), backends in by_key.items():
        if not {"hashmap", "forl0"}.issubset(backends):
            continue
        h, f = backends["hashmap"], backends["forl0"]
        output.append({
            "family": family,
            "workload": f"{scenario}:{query}" if query else scenario,
            "hashmap_throughput": h["throughput"],
            "forl0_throughput": f["throughput"],
            "throughput_change_pct": (f["throughput"] / h["throughput"] - 1.0) * 100.0,
            "hashmap_cpu": h.get("cpu_cores"),
            "forl0_cpu": f.get("cpu_cores"),
            "throughput_per_core_ratio": f["throughput_per_core"] / h["throughput_per_core"],
            "sample_note": "n=3 repeats/backend" if family == "WordCount" else "single run/backend",
        })
    order = {"WordCount": 0, "Client": 1, "NexMark": 2}
    return sorted(output, key=lambda row: (order[row["family"]], row["workload"]))


def configure_style() -> None:
    mpl.rcParams.update({
        "font.family": "DejaVu Sans",
        "font.size": 8,
        "axes.labelsize": 8,
        "axes.titlesize": 9,
        "xtick.labelsize": 7,
        "ytick.labelsize": 7,
        "legend.fontsize": 7,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
        "svg.fonttype": "none",
        "figure.dpi": 150,
        "savefig.dpi": 300,
    })


def panel_label(ax: plt.Axes, label: str) -> None:
    ax.text(-0.12, 1.06, label, transform=ax.transAxes, fontsize=10, fontweight="bold", va="top")


def save_figure(fig: plt.Figure, base: Path) -> None:
    base.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(base.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(base.with_suffix(".svg"), bbox_inches="tight")
    png = base.with_suffix(".png")
    fig.savefig(png, bbox_inches="tight", dpi=300)
    with Image.open(png) as image:
        image.convert("L").save(base.with_name(base.name + "_grayscale").with_suffix(".png"))


def figure_performance(rows: list[dict[str, Any]], pairs: list[dict[str, Any]], out: Path,
                       campaign_id: str) -> Path:
    configure_style()
    fig, axes = plt.subplots(2, 2, figsize=(7.2, 5.2), constrained_layout=True)

    ax = axes[0, 0]
    wordcount = [row for row in rows if row["family"] == "WordCount"]
    for x, backend in enumerate(("hashmap", "forl0")):
        row = next(item for item in wordcount if item["backend"] == backend)
        samples = [float(item["throughput"]) / 1e6 for item in row["repeat_samples"]]
        jitter = np.linspace(-0.04, 0.04, len(samples))
        color = HASHMAP_COLOR if backend == "hashmap" else FORL0_COLOR
        ax.scatter(np.full(len(samples), x) + jitter, samples, color=color, s=28, zorder=3,
                   marker="o" if backend == "hashmap" else "s", edgecolor="white", linewidth=0.5)
        ax.hlines(np.median(samples), x - 0.16, x + 0.16, color=color, linewidth=2)
    ax.set_xticks([0, 1], ["HashMap", "ForL0"])
    ax.set_ylabel("Throughput (M records/s)")
    ax.set_title("WordCount repeats (median line, n=3)")
    ax.grid(axis="y", color=GRID_COLOR, linewidth=0.6)
    panel_label(ax, "a")

    ax = axes[0, 1]
    app_pairs = [row for row in pairs if row["family"] in {"WordCount", "Client"}]
    labels = [
        "WordCount" if row["family"] == "WordCount" else row["workload"].replace("_", " ")
        for row in app_pairs
    ]
    changes = [row["throughput_change_pct"] for row in app_pairs]
    ypos = np.arange(len(labels))
    ax.axvline(0, color="#555555", linewidth=0.8)
    ax.scatter(changes, ypos, color=FORL0_COLOR, marker="D", s=30, zorder=3)
    for x, y in zip(changes, ypos):
        ax.text(x + (0.35 if x >= 0 else -0.35), y, f"{x:+.1f}%", va="center",
                ha="left" if x >= 0 else "right", fontsize=7)
    ax.set_yticks(ypos, labels)
    ax.set_xlabel("ForL0 throughput change vs HashMap (%)")
    ax.set_title("Application-level paired estimates")
    ax.set_xlim(min(-12, min(changes) - 2), max(3, max(changes) + 2))
    ax.grid(axis="x", color=GRID_COLOR, linewidth=0.6)
    panel_label(ax, "b")

    nex_pairs = [row for row in pairs if row["family"] == "NexMark"]
    ax = axes[1, 0]
    labels = [row["workload"].replace("forl0_tps_probe:", "").upper() for row in nex_pairs]
    changes = [row["throughput_change_pct"] for row in nex_pairs]
    ypos = np.arange(len(labels))
    ax.axvline(0, color="#555555", linewidth=0.8)
    ax.scatter(changes, ypos, color=FORL0_COLOR, marker="D", s=34, zorder=3)
    for x, y in zip(changes, ypos):
        ax.text(x + 0.5, y, f"{x:+.1f}%", va="center", fontsize=7)
    ax.set_yticks(ypos, labels)
    ax.set_xlabel("ForL0 throughput change vs HashMap (%)")
    ax.set_title("NexMark paired throughput (single runs)")
    ax.set_xlim(-3, max(changes) + 5)
    ax.grid(axis="x", color=GRID_COLOR, linewidth=0.6)
    panel_label(ax, "c")

    ax = axes[1, 1]
    for y, row in enumerate(nex_pairs):
        h, f = row["hashmap_cpu"], row["forl0_cpu"]
        ax.plot([h, f], [y, y], color="#999999", linewidth=1.3, zorder=1)
        ax.scatter(h, y, color=HASHMAP_COLOR, marker="o", s=34, label="HashMap" if y == 0 else None, zorder=3)
        ax.scatter(f, y, color=FORL0_COLOR, marker="s", s=34, label="ForL0" if y == 0 else None, zorder=3)
    ax.set_yticks(np.arange(len(labels)), labels)
    ax.set_xlabel("Reported average CPU cores (lower is better)")
    ax.set_title("NexMark CPU demand (single runs)")
    ax.grid(axis="x", color=GRID_COLOR, linewidth=0.6)
    ax.legend(frameon=False, loc="lower right")
    panel_label(ax, "d")

    fig.suptitle(f"ForL0 campaign {campaign_id}: valid paired measurements", fontsize=11, fontweight="bold")
    base = out / "forl0_performance_summary"
    save_figure(fig, base)
    plt.close(fig)
    return base.with_suffix(".png")


def figure_status(status_rows: list[dict[str, Any]], out: Path) -> Path:
    configure_style()
    pair_names = list(dict.fromkeys(row["pair"] for row in status_rows))
    matrix = np.zeros((len(pair_names), 2), dtype=int)
    reasons: dict[tuple[int, int], str] = {}
    for row in status_rows:
        i = pair_names.index(row["pair"])
        j = 0 if row["backend"] == "hashmap" else 1
        matrix[i, j] = 1 if row["status"] == "success" else 0
        reasons[(i, j)] = row["reason"]
    successful = sum(row["status"] == "success" for row in status_rows)
    complete = sum(
        all(row["status"] == "success" for row in status_rows if row["pair"] == pair)
        for pair in pair_names
    )
    fig = plt.figure(figsize=(7.2, 5.4))
    grid = fig.add_gridspec(1, 2, width_ratios=(2.15, 1.35), wspace=0.42)
    ax = fig.add_subplot(grid[0, 0])
    ax.imshow(matrix, cmap=ListedColormap([FAIL_COLOR, SUCCESS_COLOR]), vmin=0, vmax=1, aspect="auto")
    for i in range(len(pair_names)):
        for j in range(2):
            ax.text(j, i, "OK" if matrix[i, j] else "FAIL", ha="center", va="center",
                    color="white", fontsize=7, fontweight="bold")
    ax.set_xticks([0, 1], ["HashMap", "ForL0"])
    ax.set_yticks(np.arange(len(pair_names)), pair_names)
    ax.set_title(f"Execution completeness: {successful}/{len(status_rows)} cells; "
                 f"{complete}/{len(pair_names)} pairs", fontweight="bold")
    ax.set_xlabel("Backend")
    ax.set_ylabel("Planned comparison")
    ax.set_xticks(np.arange(-0.5, 2, 1), minor=True)
    ax.set_yticks(np.arange(-0.5, len(pair_names), 1), minor=True)
    ax.grid(which="minor", color="white", linewidth=1.5)
    ax.tick_params(which="minor", bottom=False, left=False)
    note = fig.add_subplot(grid[0, 1])
    note.axis("off")
    note.text(0.0, 0.98,
            "Failure classes\n"
            "- HashMap deep q18/q20: TaskManager loss\n"
            "- ForL0: strict L0 allocation\n"
            "- ForL0 q3: allocation + table capacity\n"
            "- Client scalar ForL0: strict L0 allocation",
            transform=note.transAxes, va="top", fontsize=7,
            bbox={"boxstyle": "round,pad=0.5", "facecolor": "#F5F5F5", "edgecolor": "#BBBBBB"})
    note.text(0.0, 0.48,
              "Reading guide\n"
              "Blue = valid result\n"
              "Orange = failed / missing\n"
              "Failed cells are excluded\n"
              "from performance comparisons.",
              transform=note.transAxes, va="top", fontsize=7,
              bbox={"boxstyle": "round,pad=0.5", "facecolor": "#F5F5F5", "edgecolor": "#BBBBBB"})
    fig.subplots_adjust(left=0.18, right=0.97, top=0.91, bottom=0.10)
    base = out / "forl0_execution_status"
    save_figure(fig, base)
    plt.close(fig)
    return base.with_suffix(".png")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0])
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def fmt_rate(value: float) -> str:
    if value >= 1e6:
        return f"{value / 1e6:.3f} M/s"
    if value >= 1e3:
        return f"{value / 1e3:.2f} K/s"
    return f"{value:.2f} /s"


def report_styles() -> dict[str, ParagraphStyle]:
    pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle("CJKTitle", parent=base["Title"], fontName="STSong-Light", fontSize=19,
                                leading=24, alignment=TA_CENTER, textColor=colors.HexColor("#1F3552")),
        "h1": ParagraphStyle("CJKH1", parent=base["Heading1"], fontName="STSong-Light", fontSize=14,
                             leading=18, spaceBefore=8, spaceAfter=7, textColor=colors.HexColor("#1F3552")),
        "h2": ParagraphStyle("CJKH2", parent=base["Heading2"], fontName="STSong-Light", fontSize=11,
                             leading=15, spaceBefore=7, spaceAfter=5, textColor=colors.HexColor("#365F91")),
        "body": ParagraphStyle("CJKBody", parent=base["BodyText"], fontName="STSong-Light", fontSize=9,
                               leading=14, spaceAfter=6),
        "small": ParagraphStyle("CJKSmall", parent=base["BodyText"], fontName="STSong-Light", fontSize=7.5,
                                leading=10),
    }


def table_flow(data: list[list[str]], widths: list[float], styles: dict[str, ParagraphStyle]) -> Table:
    rendered = [[Paragraph(str(cell), styles["small"]) for cell in row] for row in data]
    table = Table(rendered, colWidths=widths, repeatRows=1, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#DCE6F1")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#1F3552")),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#B8C4D1")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F7F9FB")]),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    return table


def build_pdf(path: Path, campaign: Path, pairs: list[dict[str, Any]], statuses: list[dict[str, Any]],
              performance_png: Path, status_png: Path) -> None:
    styles = report_styles()
    manifest = read_json(campaign / "run_manifest.json")
    successful = sum(row["status"] == "success" for row in statuses)
    pair_names = list(dict.fromkeys(row["pair"] for row in statuses))
    complete = sum(
        all(row["status"] == "success" for row in statuses if row["pair"] == pair)
        for pair in pair_names
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(str(path), pagesize=A4, leftMargin=18 * mm, rightMargin=18 * mm,
                            topMargin=16 * mm, bottomMargin=16 * mm,
                            title="ForL0 最新性能分析", author="ForL0 benchmark analysis")
    story: list[Any] = []
    story += [
        Paragraph("ForL0 最新性能与失败分析", styles["title"]),
        Spacer(1, 4 * mm),
        Paragraph(f"实验批次：{manifest.get('run_id')}　证据类型：real-online（部分失败）", styles["body"]),
        Paragraph("结论边界：本报告仅使用成功且配置可配对的原始样本。失败或缺失单元格不补零、不参与加速比。"
                  "除 WordCount 每个后端有 3 次重复外，其余均为单次测量，不能据此声明统计显著性。", styles["body"]),
        Paragraph("执行摘要", styles["h1"]),
        Paragraph("1. WordCount 中 ForL0 中位吞吐为 4.532M records/s，比 HashMap 低 9.1%，这是当前最明确的性能回退。", styles["body"]),
        Paragraph("2. NexMark q18 吞吐提高 17.2%；q19 吞吐持平。两者报告的 CPU 需求分别下降 75.5% 和 45.8%。"
                  "由于每个后端只有一次测量，且 q18 CPU 差异很大，这一效率结果必须复测确认。", styles["body"]),
        Paragraph("3. Client contract、optimized、state-pressure 三个成功配对的吞吐差异均小于 0.1%，表现为基本持平；"
                  "这些 bounded/auto-stop 工作负载更像正确性或源速率探针，而不是能区分后端的性能压力测试。", styles["body"]),
        Paragraph(f"4. 完整矩阵 {len(statuses)} 个单元格中仅 {successful} 个成功；{len(pair_names)} 组后端配对中仅 {complete} 组完整。失败主要来自严格 L0 分配失败，"
                  "另有 HashMap TaskManager 丢失和 ForL0 q3 表容量上限。", styles["body"]),
        Spacer(1, 2 * mm),
        Paragraph("因此，当前证据支持“部分工作负载有 CPU 效率潜力，但性能与稳定性尚未形成一致优势”，不支持整体加速结论。", styles["h2"]),
        PageBreak(),
        Paragraph("有效性能样本", styles["h1"]),
        ReportImage(str(performance_png), width=174 * mm, height=126 * mm),
        Paragraph("图 1　只展示成功且可配对的数据。点为直接测量；WordCount 显示全部 3 次重复和中位数。"
                  "其他 workload 为单次测量，无误差棒、无显著性检验。", styles["small"]),
        PageBreak(),
        Paragraph("执行完整性与失败分布", styles["h1"]),
        ReportImage(str(status_png), width=174 * mm, height=131 * mm),
        Paragraph("图 2　蓝色为成功并产生有效原始结果，橙红色为失败或无有效样本。失败单元格不进入图 1。", styles["small"]),
        PageBreak(),
        Paragraph("可配对性能明细", styles["h1"]),
    ]
    perf_table = [["类别", "Workload", "HashMap", "ForL0", "变化", "证据"]]
    for row in pairs:
        perf_table.append([
            row["family"], row["workload"], fmt_rate(row["hashmap_throughput"]),
            fmt_rate(row["forl0_throughput"]), f"{row['throughput_change_pct']:+.1f}%", row["sample_note"],
        ])
    story += [table_flow(perf_table, [18 * mm, 54 * mm, 27 * mm, 27 * mm, 18 * mm, 32 * mm], styles),
              Spacer(1, 5 * mm), Paragraph("失败明细", styles["h1"])]
    failure_table = [["Workload", "后端", "失败原因"]]
    for row in statuses:
        if row["status"] == "failed":
            failure_table.append([row["pair"], row["backend"], row["reason"]])
    story += [table_flow(failure_table, [48 * mm, 30 * mm, 98 * mm], styles), PageBreak(),
              Paragraph("分析与后续优先级", styles["h1"]),
              Paragraph("P0 - 先解决 L0 分配语义。当前 expected-engines 主要按 pipeline parallelism 设置，但 NexMark 一个作业可创建多个"
                        "有状态 operator instance，并跨两个 TaskManager 共享同一 L0 设备。应按实际并发 state-backend engine 总数对全局 L0"
                        "预算切分，并在作业启动前记录 requested/available/allocated bytes。不要仅靠增大缓存总量。", styles["body"]),
              Paragraph("P0 - 修复 q3 容量规划。q3 同时出现 L0 分配失败和 SwissTable max-table-capacity。先根据键基数与 operator 并行度"
                        "估算每实例容量，再提高上限；盲目扩大所有场景会增加内存风险。", styles["body"]),
              Paragraph("P0 - 失败必须可见。本轮 Client scalar ForL0 明确 FAILED，却未生成失败标记。代码已修改为 Client 返回空结果时"
                        "立即非零退出，使 run_all_apps 生成 FAILED_client_* 证据。", styles["body"]),
              Paragraph("P1 - 只复跑失败配对。先运行 N03-N04、N07-N08、N09-N14、C07-C08，不必重跑已经成功的完整矩阵；"
                        "每个修复后的配置至少重复 3 次，再合并到主结论。", styles["body"]),
              Paragraph("P1 - 复核 q18/q19 CPU 指标。q18 的 throughput/core 为 4.79x、q19 为 1.84x，但均为单次样本。"
                        "应记录 CPU 采样窗口、TaskManager 数量与每次原始序列，确认没有测量窗口错位。", styles["body"]),
              Paragraph("P2 - 重做 Client 性能压力轴。当前三组成功 Client 配对几乎完全持平，说明 workload 受 source/bounded shutdown"
                        "主导。应增加持续时间和状态访问密度，并分别报告输入速率、处理吞吐和状态操作吞吐。", styles["body"]),
              Paragraph("来源与可复现性", styles["h1"]),
              Paragraph(f"原始目录：{campaign.as_posix()}。run_manifest 的 control_revision 为 unavailable，这是下一轮必须修复的溯源缺口。"
                        "本 PDF 和图表是 derived-artifact，不代表重新运行实验。", styles["body"]),
    ]

    def footer(canvas, document) -> None:  # type: ignore[no-untyped-def]
        canvas.saveState()
        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(colors.HexColor("#666666"))
        canvas.drawString(18 * mm, 9 * mm, f"ForL0 campaign {manifest.get('run_id')} - derived analysis")
        canvas.drawRightString(A4[0] - 18 * mm, 9 * mm, f"Page {document.page}")
        canvas.restoreState()

    doc.build(story, onFirstPage=footer, onLaterPages=footer)


def write_markdown(path: Path, campaign: Path, pairs: list[dict[str, Any]], statuses: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# ForL0 latest performance analysis",
        "",
        f"Campaign: `{campaign.name}` (`real-online`, partial/failed).",
        "",
        "Only successful, configuration-matched backend pairs are compared. Failed cells are excluded, not treated as zero.",
        "",
        "## Paired results",
        "",
        "| Family | Workload | HashMap | ForL0 | Change | Evidence |",
        "|---|---|---:|---:|---:|---|",
    ]
    for row in pairs:
        lines.append(f"| {row['family']} | {row['workload']} | {fmt_rate(row['hashmap_throughput'])} | "
                     f"{fmt_rate(row['forl0_throughput'])} | {row['throughput_change_pct']:+.1f}% | {row['sample_note']} |")
    lines += ["", "## Failed cells", "", "| Workload | Backend | Reason |", "|---|---|---|"]
    for row in statuses:
        if row["status"] == "failed":
            lines.append(f"| {row['pair']} | {row['backend']} | {row['reason']} |")
    successful = sum(row["status"] == "success" for row in statuses)
    pair_names = list(dict.fromkeys(row["pair"] for row in statuses))
    complete = sum(
        all(row["status"] == "success" for row in statuses if row["pair"] == pair)
        for pair in pair_names
    )
    lines += [
        "", "## Interpretation", "",
        "- WordCount regresses by 9.1% for ForL0 across the reported median-of-3 result.",
        "- NexMark q18 improves throughput by 17.2%; q19 is throughput-neutral. CPU-efficiency deltas require repeated confirmation.",
        "- Three successful Client pairs are within 0.1%, indicating parity/source limitation rather than a demonstrated backend advantage.",
        f"- Matrix completeness is {successful}/{len(statuses)} cells and {complete}/{len(pair_names)} backend pairs; no whole-suite speedup claim is justified.",
        "", "This document is a derived artifact generated from existing raw results; it is not a new experiment run.", "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", type=Path, required=True,
                        help="Campaign directory containing run_manifest.json and formal/")
    parser.add_argument("--output", type=Path, default=Path("output"))
    args = parser.parse_args()
    campaign = args.campaign.resolve()
    formal = campaign / "formal"
    if not (campaign / "run_manifest.json").is_file() or not formal.is_dir():
        raise SystemExit(f"invalid campaign directory: {campaign}")
    rows = load_metrics(formal)
    index = metric_index(rows)
    statuses = matrix_rows(index)
    pairs = paired_rows(rows)
    figures = args.output / "figures"
    performance_png = figure_performance(rows, pairs, figures, campaign.name)
    status_png = figure_status(statuses, figures)
    write_csv(args.output / "data" / "campaign_metrics.csv", [
        {key: value for key, value in row.items() if key != "repeat_samples"} for row in rows
    ])
    write_csv(args.output / "data" / "execution_status.csv", statuses)
    write_csv(args.output / "data" / "paired_results.csv", pairs)
    write_markdown(args.output / "forl0_performance_analysis.md", campaign, pairs, statuses)
    build_pdf(args.output / "pdf" / "forl0_performance_analysis.pdf", campaign, pairs, statuses,
              performance_png, status_png)
    summary = {
        "campaign": campaign.name,
        "evidence_label": "derived-artifact from real-online partial campaign",
        "successful_cells": sum(row["status"] == "success" for row in statuses),
        "planned_cells": len(statuses),
        "complete_pairs": len(pairs),
        "planned_pairs": len(statuses) // 2,
    }
    (args.output / "analysis_summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
