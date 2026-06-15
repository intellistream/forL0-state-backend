#!/usr/bin/env python3
"""Flame graph quality checks to detect idle-dominated profiling captures."""

import ast
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional


IDLE_PATTERNS = (
    'thread.sleep',
    'jvm_sleep',
    'os::sleep',
    'platformevent::park',
    'pthread_cond_timedwait',
    'lightarrayrevolverscheduler',
    'waitnanos',
    'parker::park',
    'monitor::iwait',
    'unsafe_park',
    'watcherthread::run',
    'futex',
    'schedule',
)


def _is_idle_frame(frame_name: str) -> bool:
    lowered = frame_name.lower()
    return any(pattern in lowered for pattern in IDLE_PATTERNS)


def _is_wrapper_frame(frame_name: str) -> bool:
    lowered = frame_name.lower()
    return lowered in {'all', 'java/lang/thread.run'}


def _decode_cpool(cpool: List[str]) -> List[str]:
    decoded = cpool[:]
    for idx in range(1, len(decoded)):
        token = decoded[idx]
        decoded[idx] = decoded[idx - 1][: ord(token[0]) - 32] + token[1:]
    return decoded


def _parse_frames(html: str) -> Optional[Dict[str, int]]:
    cpool_match = re.search(r"const cpool = \[(.*?)\];", html, re.S)
    if not cpool_match:
        return None

    cpool_raw = ast.literal_eval('[' + cpool_match.group(1) + ']')
    cpool = _decode_cpool(cpool_raw)

    start_offset = html.find('unpack(cpool);')
    if start_offset < 0:
        return None

    ops = re.findall(r'\b([nuf])\(([^)]*)\)', html[start_offset:])
    if not ops:
        return None

    frame_max_width: Dict[str, int] = defaultdict(int)
    level0 = 0
    left0 = 0
    width0 = 0
    root_width = 0

    def _to_int(token: str) -> Optional[int]:
        token = token.strip()
        if token == '':
            return None
        return int(token, 0)

    for op, arg_str in ops:
        values = [_to_int(value) for value in arg_str.split(',')]
        key = values[0]
        if key is None:
            continue

        if op == 'f':
            level = values[1] or 0
            left = values[2] or 0
            width = values[3] if len(values) > 3 else None
        elif op == 'u':
            level = level0 + 1
            left = 0
            width = values[1] if len(values) > 1 else None
        else:
            level = level0
            left = width0
            width = values[1] if len(values) > 1 else None

        level0 = level
        left0 += left
        width0 = width if width is not None else width0
        if level == 0 and root_width == 0:
            root_width = width0

        frame_name = cpool[key >> 3]
        if width0 > frame_max_width[frame_name]:
            frame_max_width[frame_name] = width0

    if root_width <= 0:
        return None

    frame_max_width['__root__'] = root_width
    return frame_max_width


def analyze_flamegraph_quality(flamegraph_path: str, top_n: int = 8) -> Optional[Dict]:
    """Analyze a flamegraph and estimate whether it is idle-dominated.

    Returns None when the flamegraph cannot be parsed.
    """
    path = Path(flamegraph_path)
    if not path.exists():
        return None

    html = path.read_text(encoding='utf-8', errors='ignore')
    frame_max_width = _parse_frames(html)
    if not frame_max_width:
        return None

    root = frame_max_width.pop('__root__', 0)
    if root <= 0:
        return None

    idle_ratio = 0.0
    for frame_name, width in frame_max_width.items():
        if _is_idle_frame(frame_name):
            ratio = width / root
            if ratio > idle_ratio:
                idle_ratio = ratio

    non_idle = [
        (frame_name, width / root)
        for frame_name, width in frame_max_width.items()
        if not _is_idle_frame(frame_name) and not _is_wrapper_frame(frame_name)
    ]
    non_idle.sort(key=lambda item: item[1], reverse=True)

    return {
        'file': str(path),
        'idle_ratio': idle_ratio,
        'idle_dominated': idle_ratio >= 0.60,
        'top_non_idle': [
            {'frame': frame_name, 'ratio': ratio}
            for frame_name, ratio in non_idle[:top_n]
        ],
    }
