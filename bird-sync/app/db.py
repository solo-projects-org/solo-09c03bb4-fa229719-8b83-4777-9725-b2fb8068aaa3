"""SQLite 持久化层：操作日志、裁定日志与记录对账（冲突检测）。

设计要点
--------
* ``ops`` 为只追加的不可变更正/删除日志，每条带站点向量时钟；
* ``resolutions`` 为裁定日志，同步时按 (向量时钟, 时间, id) 确定性择优，每记录每站至多一条；
* 记录当前状态由 ``_reconcile`` 从操作日志+裁定重算，因此同步可重复、乱序、中断重试，
  收齐相同日志的两站必然得到相同记录与冲突；
* 批次在单个 ``BEGIN IMMEDIATE`` 事务内写入，任何失败整批回滚。
"""

from __future__ import annotations

import csv
import functools
import json
import re
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Optional

from .vv import Vv, copy_vv, merge_many, vv_equal, vv_lt

# FastAPI 同步端点跑在线程池中，所有写事务经此锁串行化，
# 避免同一连接上并发 BEGIN IMMEDIATE 互相破坏事务状态。
write_lock = threading.RLock()


def _locked(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        with write_lock:
            return fn(*args, **kwargs)

    return wrapper

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    station_id TEXT NOT NULL,
    vv TEXT NOT NULL,
    seq INTEGER NOT NULL,
    res_seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS records (
    record_id TEXT PRIMARY KEY,
    site TEXT NOT NULL,
    species TEXT NOT NULL,
    seed_count INTEGER NOT NULL,
    count INTEGER,
    deleted INTEGER NOT NULL DEFAULT 0,
    version_vv TEXT,
    conflicted INTEGER NOT NULL DEFAULT 0,
    has_resolution INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS ops (
    id TEXT PRIMARY KEY,
    record_id TEXT NOT NULL,
    station_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('count', 'delete')),
    count INTEGER,
    vv TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ops_record ON ops(record_id);
CREATE TABLE IF NOT EXISTS resolutions (
    id TEXT PRIMARY KEY,
    record_id TEXT NOT NULL,
    station_id TEXT NOT NULL,
    choice TEXT NOT NULL CHECK (choice IN ('count', 'delete')),
    count INTEGER,
    vv TEXT NOT NULL,
    read_max_vv TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_res_record ON resolutions(record_id);
"""


class StaleBranch(Exception):
    """读取后分支已变化，旧裁定/旧更正被拒绝（HTTP 409）。"""


class BatchRejected(Exception):
    """批次校验失败，整批回滚（HTTP 400）。"""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dump(vv: Vv) -> str:
    return json.dumps({k: v for k, v in vv.items() if v}, sort_keys=True)


def _load(text: Optional[str]) -> Vv:
    if not text:
        return {}
    return {k: int(v) for k, v in json.loads(text).items()}


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


@_locked
def init_db(conn: sqlite3.Connection, station_id: str, csv_path: str) -> None:
    """建库并以同一份 CSV 初始化（仅首次；重启保留全部数据）。"""
    conn.executescript(SCHEMA)
    row = conn.execute("SELECT station_id FROM meta WHERE id = 1").fetchone()
    if row is not None:
        if row["station_id"] != station_id:
            raise RuntimeError(
                f"数据卷属于站点 {row['station_id']}，不能以 {station_id} 身份启动"
            )
        return

    rows: list[tuple[str, str, str, int]] = []
    seen: set[str] = set()
    with open(csv_path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        required = {"record_id", "site", "species", "count"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise RuntimeError(f"种子 CSV 缺少列: {sorted(missing)}")
        for line, raw in enumerate(reader, start=2):
            rid = (raw["record_id"] or "").strip()
            site = (raw["site"] or "").strip()
            species = (raw["species"] or "").strip()
            if not rid or rid in seen:
                raise RuntimeError(f"种子 CSV 第 {line} 行记录号为空或重复: {rid!r}")
            seen.add(rid)
            try:
                count = int(str(raw["count"]).strip())
            except (TypeError, ValueError):
                raise RuntimeError(f"种子 CSV 第 {line} 行计数不是整数: {raw['count']!r}")
            if count < 0:
                raise RuntimeError(f"种子 CSV 第 {line} 行计数为负: {count}")
            rows.append((rid, site, species, count))

    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "INSERT INTO meta (id, station_id, vv, seq, res_seq) VALUES (1, ?, '{}', 0, 0)",
            (station_id,),
        )
        conn.executemany(
            "INSERT INTO records (record_id, site, species, seed_count, count, deleted) "
            "VALUES (?, ?, ?, ?, ?, 0)",
            [(r, s, sp, c, c) for (r, s, sp, c) in rows],
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


# --------------------------------------------------------------------------- 元数据


def get_meta(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM meta WHERE id = 1").fetchone()
    return {
        "station_id": row["station_id"],
        "vv": _load(row["vv"]),
        "seq": row["seq"],
        "res_seq": row["res_seq"],
    }


def _tick(conn: sqlite3.Connection) -> tuple[str, int, Vv]:
    """产生本站下一个操作序号与新向量时钟（仅本地写操作调用）。"""
    meta = get_meta(conn)
    seq = meta["seq"] + 1
    vv = copy_vv(meta["vv"])
    vv[meta["station_id"]] = seq
    conn.execute(
        "UPDATE meta SET vv = ?, seq = ? WHERE id = 1", (_dump(vv), seq)
    )
    return meta["station_id"], seq, vv


# --------------------------------------------------------------------------- 对账


def _ops_of(conn: sqlite3.Connection, record_id: str) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            "SELECT * FROM ops WHERE record_id = ? ORDER BY station_id, id", (record_id,)
        )
    )


def _maximals(ops: list[sqlite3.Row]) -> list[sqlite3.Row]:
    """极大操作：不存在因果后继的操作。"""
    out: list[sqlite3.Row] = []
    for op in ops:
        vv = _load(op["vv"])
        dominated = False
        for other in ops:
            if other["id"] == op["id"]:
                continue
            if vv_lt(vv, _load(other["vv"])):
                dominated = True
                break
        if not dominated:
            out.append(op)
    return out


def _divergent(maximals: list[sqlite3.Row]) -> bool:
    """极大分支之间是否存在实质分歧（全删、或同值计数视为自动合并）。"""
    if len(maximals) <= 1:
        return False
    types = {m["type"] for m in maximals}
    if types == {"delete"}:
        return False
    if types == {"count"}:
        counts = {m["count"] for m in maximals}
        return len(counts) > 1
    return True  # 删除与改值并存


def _resolution_matches(
    res: sqlite3.Row, maximals: list[sqlite3.Row]
) -> tuple[bool, Optional[sqlite3.Row]]:
    """裁定选择是否与当前某个极大分支一致；一致时返回该分支作为生效值。"""
    if res["choice"] == "delete":
        branch = next((m for m in maximals if m["type"] == "delete"), None)
        return (branch is not None), branch
    for m in maximals:
        if m["type"] == "count" and m["count"] == res["count"]:
            return True, m
    return False, None


def _reconcile(conn: sqlite3.Connection, record_id: str) -> None:
    """依据操作日志与裁定重算单条记录的当前状态与冲突标记。

    * 极大并发分支无实质分歧（全删 / 同值计数）：自动合并，无冲突；
    * 有实质分歧：列为冲突；存在新鲜且选择合法的裁定时裁定生效，解除冲突；
    * 裁定陈旧（分支被后继取代，或出现未见的并发更正）：重新列为冲突。
    """
    rec = conn.execute(
        "SELECT * FROM records WHERE record_id = ?", (record_id,)
    ).fetchone()
    if rec is None:
        return
    ops = _ops_of(conn, record_id)
    res = conn.execute(
        "SELECT * FROM resolutions WHERE record_id = ? ORDER BY id LIMIT 1",
        (record_id,),
    ).fetchone()

    has_resolution = res is not None
    conflicted = False
    effective = False
    chosen: Optional[sqlite3.Row] = None

    if not ops:
        # 只有种子值（裁定先于操作到达时同样落回此分支，操作到达后会再次对账）
        count, deleted, version_vv = rec["seed_count"], 0, None
    else:
        maximals = _maximals(ops)
        if len(maximals) == 1:
            # 单一当前分支：后继更正自动合并，并静默覆盖任何旧裁定
            chosen = maximals[0]
        elif not _divergent(maximals):
            # 多个极大分支但无实质分歧（全删或同值）：确定性取代表，自动合并
            chosen = maximals[0]
        else:
            conflicted = True
            if res is not None and _resolution_fresh(res, maximals):
                effective, chosen = _resolution_matches(res, maximals)
            # 无裁定 / 裁定陈旧 / 裁定选择已不在分支中：保持冲突

        if chosen is not None:
            count = chosen["count"]
            deleted = 1 if chosen["type"] == "delete" else 0
            version_vv = _load(chosen["vv"])
        else:
            # 冲突未裁定或裁定失效：保留确定性的代表值仅用于展示，分析接口会排除
            count_branch = next((m for m in maximals if m["type"] == "count"), None)
            count = count_branch["count"] if count_branch else maximals[0]["count"]
            deleted = 0 if count_branch else 1
            version_vv = None

    # 生效裁定解除冲突标记；陈旧裁定保留冲突，只在冲突详情中标记状态
    if conflicted and effective:
        conflicted = False

    conn.execute(
        "UPDATE records SET count = ?, deleted = ?, version_vv = ?, conflicted = ?, "
        "has_resolution = ? WHERE record_id = ?",
        (
            count,
            deleted,
            _dump(version_vv) if version_vv is not None else None,
            1 if conflicted else 0,
            1 if has_resolution else 0,
            record_id,
        ),
    )


def _resolution_fresh(res: sqlite3.Row, maximals: list[sqlite3.Row]) -> bool:
    """裁定读取的极大分支集合与当前极大分支集合完全一致才算新鲜。

    出现任何未见的并发更正（新的极大分支），或旧分支被后继取代，
    都会使集合不一致 -> 裁定失效，重新列为冲突。
    """
    read_vvs = {_dump(dict(x)) for x in json.loads(res["read_max_vv"])}
    current_vvs = {_dump(_load(m["vv"])) for m in maximals}
    return read_vvs == current_vvs


# --------------------------------------------------------------------------- 本地写


def _check_branch(
    conn: sqlite3.Connection, record_id: str, expected_vv: Vv
) -> None:
    ops = _ops_of(conn, record_id)
    maximals = _maximals(ops)
    if not any(vv_equal(expected_vv, _load(m["vv"])) for m in maximals):
        raise StaleBranch(
            "读取后该记录分支已变化（出现因果后继或冲突已裁定），基于旧状态的提交被拒绝"
        )


def _insert_op(
    conn: sqlite3.Connection,
    record_id: str,
    op_type: str,
    count: Optional[int],
) -> dict[str, Any]:
    if not conn.execute(
        "SELECT 1 FROM records WHERE record_id = ?", (record_id,)
    ).fetchone():
        raise KeyError(record_id)
    station_id, seq, vv = _tick(conn)
    op_id = f"{station_id}-{seq}"
    conn.execute(
        "INSERT INTO ops (id, record_id, station_id, seq, type, count, vv, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            op_id,
            record_id,
            station_id,
            seq,
            op_type,
            count,
            _dump(vv),
            utc_now_iso(),
        ),
    )
    return {"id": op_id, "vv": vv}


@_locked
def correct(
    conn: sqlite3.Connection,
    record_id: str,
    count: int,
    expected_branch_vv: Optional[Vv] = None,
) -> dict[str, Any]:
    conn.execute("BEGIN IMMEDIATE")
    try:
        if expected_branch_vv is not None:
            _check_branch(conn, record_id, expected_branch_vv)
        info = _insert_op(conn, record_id, "count", count)
        _reconcile(conn, record_id)
        conn.execute("COMMIT")
        return info
    except Exception:
        conn.execute("ROLLBACK")
        raise


@_locked
def delete_record(
    conn: sqlite3.Connection,
    record_id: str,
    expected_branch_vv: Optional[Vv] = None,
) -> dict[str, Any]:
    conn.execute("BEGIN IMMEDIATE")
    try:
        if expected_branch_vv is not None:
            _check_branch(conn, record_id, expected_branch_vv)
        info = _insert_op(conn, record_id, "delete", None)
        _reconcile(conn, record_id)
        conn.execute("COMMIT")
        return info
    except Exception:
        conn.execute("ROLLBACK")
        raise


# --------------------------------------------------------------------------- 裁定


@_locked
def resolve(
    conn: sqlite3.Connection,
    record_id: str,
    choice: str,
    count: Optional[int],
    expected_branch_vv: Optional[Vv] = None,
) -> dict[str, Any]:
    conn.execute("BEGIN IMMEDIATE")
    try:
        rec = conn.execute(
            "SELECT * FROM records WHERE record_id = ?", (record_id,)
        ).fetchone()
        if rec is None:
            raise KeyError(record_id)

        ops = _ops_of(conn, record_id)
        maximals = _maximals(ops)
        if len(maximals) < 2 or not _divergent(maximals):
            raise StaleBranch("该记录当前没有待裁定的冲突")

        if expected_branch_vv is not None and not any(
            vv_equal(expected_branch_vv, _load(m["vv"])) for m in maximals
        ):
            raise StaleBranch("读取后分支有变，旧裁定被拒绝")

        if choice == "count":
            if count is None or count < 0:
                raise BatchRejected("choice=count 时必须提供非负整数 count")
            if not any(
                m["type"] == "count" and m["count"] == count for m in maximals
            ):
                raise BatchRejected("裁定计数必须是当前某个冲突分支上的计数")
        else:
            if not any(m["type"] == "delete" for m in maximals):
                raise BatchRejected("当前冲突分支中没有删除操作，不能裁定为删除")
            count = None

        meta = get_meta(conn)
        res_seq = meta["res_seq"] + 1
        res_id = f"{meta['station_id']}-r{res_seq}"
        read_max_vv = [_load(m["vv"]) for m in maximals]
        # 裁定推进专用时钟分量 "<station>:r"，使裁定可被同步过滤器识别为新知识
        res_vv = copy_vv(meta["vv"])
        res_vv[f"{meta['station_id']}:r"] = res_seq
        conn.execute(
            "UPDATE meta SET res_seq = ?, vv = ? WHERE id = 1",
            (res_seq, _dump(res_vv)),
        )
        # 本站旧裁定被新裁定取代
        conn.execute(
            "DELETE FROM resolutions WHERE record_id = ? AND station_id = ?",
            (record_id, meta["station_id"]),
        )
        conn.execute(
            "INSERT INTO resolutions (id, record_id, station_id, choice, count, vv, "
            "read_max_vv, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                res_id,
                record_id,
                meta["station_id"],
                choice,
                count,
                _dump(res_vv),
                json.dumps(read_max_vv),
                utc_now_iso(),
            ),
        )
        _reconcile(conn, record_id)
        conn.execute("COMMIT")
        return {"id": res_id, "read_max_vv": read_max_vv}
    except Exception:
        conn.execute("ROLLBACK")
        raise


# --------------------------------------------------------------------------- 同步批次


def _validate_op_payload(data: dict[str, Any], known_records: set[str]) -> None:
    rid = data.get("record_id")
    if rid not in known_records:
        raise BatchRejected(f"批次引用未知记录号: {rid!r}")
    sid = data.get("station_id")
    seq = data.get("seq")
    if not isinstance(sid, str) or not sid:
        raise BatchRejected("操作缺少 station_id")
    if not isinstance(seq, int) or isinstance(seq, bool) or seq < 1:
        raise BatchRejected(f"操作 {data.get('id')} 的 seq 非法")
    if data.get("id") != f"{sid}-{seq}":
        raise BatchRejected(
            f"操作 id {data.get('id')!r} 与 station_id/seq 不一致"
        )
    vv = data.get("vv")
    if not isinstance(vv, dict) or not all(
        isinstance(k, str) and isinstance(v, int) and v >= 0 for k, v in vv.items()
    ):
        raise BatchRejected(f"操作 {data.get('id')} 的向量时钟非法")
    if vv.get(sid) != seq:
        raise BatchRejected(
            f"操作 {data.get('id')} 的向量时钟本站分量 {vv.get(sid)} 不等于 seq {seq}"
        )
    if data.get("type") == "count":
        c = data.get("count")
        if not isinstance(c, int) or isinstance(c, bool) or c < 0:
            raise BatchRejected(f"操作 {data.get('id')} 的 count 必须为非负整数")
    elif data.get("type") == "delete":
        if data.get("count") is not None:
            raise BatchRejected(f"删除操作 {data.get('id')} 不能带 count")
    else:
        raise BatchRejected(f"操作 {data.get('id')} 的 type 非法")
    try:
        datetime.fromisoformat(data["created_at"])
    except (KeyError, TypeError, ValueError):
        raise BatchRejected(f"操作 {data.get('id')} 的 created_at 非法")


def _as_vv(value) -> Vv:
    """sqlite Row 中存的是 JSON 字符串，入站批次载荷是 dict，统一成 dict。"""
    return dict(value) if isinstance(value, dict) else _load(value)


def _compare_resolutions(a: sqlite3.Row | dict, b: sqlite3.Row | dict) -> int:
    """返回正数表示 a 应胜出。因果优先（vv_lt），并发按时间、id 确定性破平。"""
    va, vb = _as_vv(a["vv"]), _as_vv(b["vv"])
    if va != vb:
        if vv_lt(vb, va):
            return 1
        if vv_lt(va, vb):
            return -1
    # 并发或同向量时钟
    ta, tb = a["created_at"], b["created_at"]
    if ta != tb:
        return 1 if ta > tb else -1
    return 1 if a["id"] > b["id"] else -1


@_locked
def apply_batch(
    conn: sqlite3.Connection,
    ops_in: list[dict[str, Any]],
    resolutions_in: list[dict[str, Any]],
) -> dict[str, int]:
    known = {
        r["record_id"]
        for r in conn.execute("SELECT record_id FROM records").fetchall()
    }
    conn.execute("BEGIN IMMEDIATE")
    dup_ops = dup_res = new_ops = new_res = 0
    affected: set[str] = set()
    try:
        # --- 操作：按 id 幂等去重；同 id 载荷不一致整批拒绝 ---
        for data in ops_in:
            _validate_op_payload(data, known)
            existing = conn.execute(
                "SELECT * FROM ops WHERE id = ?", (data["id"],)
            ).fetchone()
            payload = (
                data["record_id"],
                data["station_id"],
                data["seq"],
                data["type"],
                data["count"],
                _dump(data["vv"]),
                data["created_at"],
            )
            if existing is not None:
                old = (
                    existing["record_id"],
                    existing["station_id"],
                    existing["seq"],
                    existing["type"],
                    existing["count"],
                    existing["vv"],
                    existing["created_at"],
                )
                if old != payload:
                    raise BatchRejected(
                        f"操作 id {data['id']} 已存在但载荷不一致，整批回滚"
                    )
                dup_ops += 1
                continue
            conn.execute(
                "INSERT INTO ops (id, record_id, station_id, seq, type, count, vv, "
                "created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    data["id"],
                    data["record_id"],
                    data["station_id"],
                    data["seq"],
                    data["type"],
                    data["count"],
                    _dump(data["vv"]),
                    data["created_at"],
                ),
            )
            new_ops += 1
            affected.add(data["record_id"])

        # --- 裁定：同记录按 (vv, 时间, id) 确定性择优，保证两站收敛 ---
        for data in resolutions_in:
            if data.get("record_id") not in known:
                raise BatchRejected(
                    f"裁定引用未知记录号: {data.get('record_id')!r}"
                )
            res_sid = data.get("station_id")
            res_id = data.get("id")
            if not isinstance(res_sid, str) or not res_sid:
                raise BatchRejected("裁定缺少 station_id")
            # 完整 id 形如 "<station>-r<seq>"，跨站天然不撞 ID
            if not isinstance(res_id, str) or not re.fullmatch(
                rf"{re.escape(res_sid)}-r\d+", res_id
            ):
                raise BatchRejected(
                    f"裁定 id {res_id!r} 与 station_id 不一致，整批回滚"
                )
            if data.get("choice") not in ("count", "delete"):
                raise BatchRejected(f"裁定 {data.get('id')} 的 choice 非法")
            if data["choice"] == "count" and (
                not isinstance(data.get("count"), int)
                or isinstance(data.get("count"), bool)
                or data["count"] < 0
            ):
                raise BatchRejected(f"裁定 {data.get('id')} 的 count 非法")
            vv = data.get("vv")
            read_vvs = data.get("read_max_vv")
            if not isinstance(vv, dict) or not isinstance(read_vvs, list):
                raise BatchRejected(f"裁定 {data.get('id')} 的向量字段非法")
            try:
                datetime.fromisoformat(data["created_at"])
            except (KeyError, TypeError, ValueError):
                raise BatchRejected(f"裁定 {data.get('id')} 的 created_at 非法")

            existing = conn.execute(
                "SELECT * FROM resolutions WHERE id = ?", (data["id"],)
            ).fetchone()
            if existing is not None:
                same = (
                    existing["record_id"] == data["record_id"]
                    and existing["station_id"] == data["station_id"]
                    and existing["choice"] == data["choice"]
                    and existing["count"] == data["count"]
                    and existing["vv"] == _dump(vv)
                    and json.loads(existing["read_max_vv"]) == read_vvs
                )
                if not same:
                    raise BatchRejected(
                        f"裁定 id {data['id']} 已存在但载荷不一致，整批回滚"
                    )
                dup_res += 1
                continue

            winner = conn.execute(
                "SELECT * FROM resolutions WHERE record_id = ? ORDER BY id LIMIT 1",
                (data["record_id"],),
            ).fetchone()
            if winner is not None and _compare_resolutions(winner, data) >= 0:
                # 已有裁定因果不落后或确定性排序更优：保留旧裁定
                continue
            if winner is not None:
                conn.execute(
                    "DELETE FROM resolutions WHERE record_id = ?",
                    (data["record_id"],),
                )
            conn.execute(
                "INSERT INTO resolutions (id, record_id, station_id, choice, count, "
                "vv, read_max_vv, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    data["id"],
                    data["record_id"],
                    data["station_id"],
                    data["choice"],
                    data["count"],
                    _dump(vv),
                    json.dumps(read_vvs),
                    data["created_at"],
                ),
            )
            new_res += 1
            affected.add(data["record_id"])

        for rid in affected:
            _reconcile(conn, rid)

        # 把入站日志携带的向量时钟并入本站时钟（只合并不递增本站序号），
        # 使两站在收齐相同日志后拥有完全相同的向量时钟，同步得以收敛。
        # 注意：即使裁定因确定性排序落选，其向量时钟仍代表“已收到的知识”，必须并入。
        incoming_vvs = [d["vv"] for d in ops_in] + [
            d["vv"] for d in resolutions_in
        ]
        if incoming_vvs:
            meta_row = conn.execute("SELECT vv FROM meta WHERE id = 1").fetchone()
            merged = merge_many([_load(meta_row["vv"])] + incoming_vvs)
            conn.execute(
                "UPDATE meta SET vv = ? WHERE id = 1", (_dump(merged),)
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return {
        "applied_ops": new_ops,
        "applied_resolutions": new_res,
        "duplicate_ops": dup_ops,
        "duplicate_resolutions": dup_res,
    }


def pull_since(
    conn: sqlite3.Connection, since: Optional[Vv]
) -> dict[str, list[dict[str, Any]]]:
    """返回严格处于 ``since`` 因果未来的操作与裁定。

    判定：``vv <= since`` 表示调用方已因果知晓，跳过；否则返回。
    这样调用方自己产生、对站不知晓的操作不会被反向重复推送。
    """
    from .vv import vv_le

    since = since or {}
    ops_out: list[dict[str, Any]] = []
    for row in conn.execute("SELECT * FROM ops ORDER BY id").fetchall():
        vv = _load(row["vv"])
        if not vv_le(vv, since):
            ops_out.append(_op_dict(row))
    res_out: list[dict[str, Any]] = []
    for row in conn.execute(
        "SELECT * FROM resolutions ORDER BY id"
    ).fetchall():
        vv = _load(row["vv"])
        if not vv_le(vv, since):
            res_out.append(_res_dict(row))
    return {"ops": ops_out, "resolutions": res_out}


# --------------------------------------------------------------------------- 查询


def _op_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "record_id": row["record_id"],
        "station_id": row["station_id"],
        "seq": row["seq"],
        "type": row["type"],
        "count": row["count"],
        "vv": _load(row["vv"]),
        "created_at": row["created_at"],
    }


def _res_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "record_id": row["record_id"],
        "station_id": row["station_id"],
        "choice": row["choice"],
        "count": row["count"],
        "vv": _load(row["vv"]),
        "read_max_vv": json.loads(row["read_max_vv"]),
        "created_at": row["created_at"],
    }


def list_records(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    out = []
    for row in conn.execute("SELECT * FROM records ORDER BY record_id").fetchall():
        out.append(
            {
                "record_id": row["record_id"],
                "site": row["site"],
                "species": row["species"],
                "count": row["count"],
                "deleted": bool(row["deleted"]),
                "conflicted": bool(row["conflicted"]),
                "version_vv": _load(row["version_vv"]) if row["version_vv"] else None,
                "has_resolution": bool(row["has_resolution"]),
            }
        )
    return out


def get_conflicts(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """返回当前仍标记为冲突的记录（含陈旧/未生效裁定的状态说明）。"""
    out = []
    rows = conn.execute(
        "SELECT * FROM records WHERE conflicted = 1 ORDER BY record_id"
    ).fetchall()
    for rec in rows:
        maximals = _maximals(_ops_of(conn, rec["record_id"]))
        res = conn.execute(
            "SELECT * FROM resolutions WHERE record_id = ? ORDER BY id LIMIT 1",
            (rec["record_id"],),
        ).fetchone()
        fresh = res is not None and _resolution_fresh(res, maximals)
        effective = False
        if fresh:
            effective, _ = _resolution_matches(res, maximals)
        out.append(
            {
                "record_id": rec["record_id"],
                "site": rec["site"],
                "species": rec["species"],
                "active": bool(rec["conflicted"]),
                "branches": [
                    {
                        "op_id": m["id"],
                        "station_id": m["station_id"],
                        "type": m["type"],
                        "count": m["count"],
                        "vv": _load(m["vv"]),
                        "created_at": m["created_at"],
                    }
                    for m in maximals
                ],
                "resolution": _res_dict(res) if res else None,
                "resolution_effective": effective,
                "resolution_stale": res is not None and not fresh,
            }
        )
    return out


def analysis_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [
        {
            "record_id": r["record_id"],
            "site": r["site"],
            "species": r["species"],
            "count": r["count"],
        }
        for r in conn.execute(
            "SELECT * FROM records WHERE conflicted = 0 AND deleted = 0 "
            "ORDER BY record_id"
        ).fetchall()
    ]


def status(conn: sqlite3.Connection) -> dict[str, Any]:
    meta = get_meta(conn)
    return {
        "station_id": meta["station_id"],
        "version_vv": meta["vv"],
        "op_count": conn.execute("SELECT COUNT(*) c FROM ops").fetchone()["c"],
        "resolution_count": conn.execute(
            "SELECT COUNT(*) c FROM resolutions"
        ).fetchone()["c"],
        "record_count": conn.execute("SELECT COUNT(*) c FROM records").fetchone()[
            "c"
        ],
        "conflict_count": conn.execute(
            "SELECT COUNT(*) c FROM records WHERE conflicted = 1"
        ).fetchone()["c"],
    }
