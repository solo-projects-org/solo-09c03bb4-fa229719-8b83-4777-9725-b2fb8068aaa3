"""向量时钟（version vector）工具。

约定：向量时钟只在“本地写操作/裁定”时随本站分量递增；纯接收批次不递增，
从而保证同一操作在两站得到完全相同的向量时钟，同步结果可收敛且可复现。
"""

from __future__ import annotations

from typing import Iterable

Vv = dict[str, int]


def copy_vv(vv: Vv) -> Vv:
    return dict(vv)


def merge_vv(a: Vv, b: Vv) -> Vv:
    out = dict(a)
    for k, v in b.items():
        if v > out.get(k, 0):
            out[k] = v
    return out


def merge_many(vvs: Iterable[Vv]) -> Vv:
    out: Vv = {}
    for vv in vvs:
        out = merge_vv(out, vv)
    return out


def vv_le(a: Vv, b: Vv) -> bool:
    """a 是否因果先于或等于 b（a <= b）。"""
    for k, v in a.items():
        if v > b.get(k, 0):
            return False
    return True


def vv_lt(a: Vv, b: Vv) -> bool:
    return vv_le(a, b) and a != b


def vv_concurrent(a: Vv, b: Vv) -> bool:
    return not vv_le(a, b) and not vv_le(b, a)


def vv_equal(a: Vv, b: Vv) -> bool:
    # 去掉为 0 的分量后比较
    norm = lambda v: {k: x for k, x in v.items() if x}
    return norm(a) == norm(b)
