"""FastAPI 应用工厂：双站离线计数同步服务。

环境变量（uvicorn 启动 ``app.main:app`` 时读取）
------------------------------------------------
* ``STATION_ID``  本站标识（alpha / bravo）
* ``DB_PATH``     SQLite 数据文件（独立数据卷，重启保留全部数据）
* ``CSV_PATH``    种子 CSV（两站同一份）
* ``PEER_URL``    对站地址，用于一键双向同步
"""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import asynccontextmanager

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.responses import RedirectResponse

from . import db
from .schemas import (
    AnalysisRow,
    BatchBody,
    BatchResult,
    ConflictOut,
    CorrectBody,
    DeleteBody,
    RecordOut,
    ResolveBody,
    StatusOut,
    SyncResult,
)
from .vv import vv_equal

MAX_SYNC_ROUNDS = 20


def create_app(
    station_id: str,
    db_path: str,
    csv_path: str,
    peer_url: str = "",
    peer_transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        conn = db.connect(db_path)
        db.init_db(conn, station_id, csv_path)
        app.state.conn = conn
        yield
        conn.close()

    app = FastAPI(
        title=f"候鸟计数同步 API — 站点 {station_id}",
        description=(
            "离线营地样点计数更正、删除、冲突裁定与双站同步。\n\n"
            "- 只允许改计数或删除，全部操作进入带向量时钟的只追加日志；\n"
            "- 双向同步按批次幂等写入，可重复、乱序、中断重试；\n"
            "- 分析接口只返回无冲突且未删除的计数。"
        ),
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.station_id = station_id
    app.state.peer_url = peer_url
    app.state.peer_transport = peer_transport

    def get_conn() -> sqlite3.Connection:
        return app.state.conn

    def _handle_write_error(exc: Exception):
        if isinstance(exc, db.StaleBranch):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
        if isinstance(exc, db.BatchRejected):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
            )
        if isinstance(exc, KeyError):
            raise HTTPException(
                status_code=404, detail=f"记录不存在: {exc.args[0]}"
            )
        raise exc

    # ---------------------------------------------------------------- 健康与状态

    @app.get("/", include_in_schema=False)
    def root():
        return RedirectResponse(url="/docs")

    @app.get("/healthz", tags=["meta"], summary="健康检查")
    def healthz():
        return {"status": "ok", "station_id": station_id}

    @app.get(
        "/status",
        response_model=StatusOut,
        tags=["meta"],
        summary="站点状态与向量时钟",
    )
    def get_status(conn: sqlite3.Connection = Depends(get_conn)):
        return db.status(conn)

    # ---------------------------------------------------------------- 记录与更正

    @app.get(
        "/records",
        response_model=list[RecordOut],
        tags=["records"],
        summary="全部记录（含删除/冲突标记与当前生效值）",
    )
    def list_records(conn: sqlite3.Connection = Depends(get_conn)):
        return db.list_records(conn)

    @app.get(
        "/records/{record_id}",
        response_model=RecordOut,
        tags=["records"],
        summary="单条记录详情",
    )
    def get_record(record_id: str, conn: sqlite3.Connection = Depends(get_conn)):
        for rec in db.list_records(conn):
            if rec["record_id"] == record_id:
                return rec
        raise HTTPException(status_code=404, detail=f"记录不存在: {record_id}")

    @app.post(
        "/records/{record_id}/corrections",
        tags=["records"],
        status_code=status.HTTP_201_CREATED,
        summary="更正计数（只追加，不改写历史）",
        responses={
            404: {"description": "记录不存在"},
            409: {"description": "分支已变化，基于旧状态的提交被拒绝"},
        },
    )
    def correct_count(
        record_id: str,
        body: CorrectBody,
        conn: sqlite3.Connection = Depends(get_conn),
    ):
        try:
            info = db.correct(conn, record_id, body.count, body.expected_branch_vv)
        except Exception as exc:  # noqa: BLE001 - 统一映射
            _handle_write_error(exc)
        return {
            "op_id": info["id"],
            "record_id": record_id,
            "type": "count",
            "count": body.count,
            "version_vv": info["vv"],
        }

    @app.delete(
        "/records/{record_id}",
        tags=["records"],
        status_code=status.HTTP_200_OK,
        summary="删除记录（墓碑同步，两站分析接口均不再返回该记录）",
        responses={
            404: {"description": "记录不存在"},
            409: {"description": "分支已变化，基于旧状态的提交被拒绝"},
        },
    )
    def delete_record(
        record_id: str,
        body: DeleteBody | None = None,
        conn: sqlite3.Connection = Depends(get_conn),
    ):
        expected = body.expected_branch_vv if body else None
        try:
            info = db.delete_record(conn, record_id, expected)
        except Exception as exc:  # noqa: BLE001
            _handle_write_error(exc)
        return {
            "op_id": info["id"],
            "record_id": record_id,
            "type": "delete",
            "version_vv": info["vv"],
        }

    # ---------------------------------------------------------------- 冲突与裁定

    @app.get(
        "/conflicts",
        response_model=list[ConflictOut],
        tags=["conflicts"],
        summary="全部冲突（含已裁定的生效/失效状态）",
    )
    def list_conflicts(conn: sqlite3.Connection = Depends(get_conn)):
        return db.get_conflicts(conn)

    @app.post(
        "/conflicts/{record_id}/resolve",
        tags=["conflicts"],
        status_code=status.HTTP_201_CREATED,
        summary="裁定冲突：选择某个计数分支，或删除",
        responses={
            404: {"description": "记录不存在"},
            409: {"description": "读取后分支有变，旧裁定被拒绝；或当前无待裁定冲突"},
            400: {"description": "裁定值不在当前冲突分支中"},
        },
    )
    def resolve_conflict(
        record_id: str,
        body: ResolveBody,
        conn: sqlite3.Connection = Depends(get_conn),
    ):
        try:
            info = db.resolve(
                conn, record_id, body.choice, body.count, body.expected_branch_vv
            )
        except Exception as exc:  # noqa: BLE001
            _handle_write_error(exc)
        return {
            "resolution_id": info["id"],
            "record_id": record_id,
            "choice": body.choice,
            "count": body.count,
            "read_max_vv": info["read_max_vv"],
        }

    # ---------------------------------------------------------------- 分析取数

    @app.get(
        "/analysis/counts",
        response_model=list[AnalysisRow],
        tags=["analysis"],
        summary="种群分析取数：仅无冲突且未删除的计数",
    )
    def analysis_counts(conn: sqlite3.Connection = Depends(get_conn)):
        return db.analysis_rows(conn)

    # ---------------------------------------------------------------- 同步协议

    @app.get(
        "/sync/pull",
        response_model=BatchBody,
        tags=["sync"],
        summary="拉取：返回向量时钟 since 因果未来的全部操作与裁定",
    )
    def sync_pull(
        since: str | None = Query(
            default=None,
            description='调用方向量时钟的 JSON 字符串，如 {"alpha":2}；缺省表示全量',
        ),
        conn: sqlite3.Connection = Depends(get_conn),
    ):
        if since:
            try:
                since_vv = json.loads(since)
                if not isinstance(since_vv, dict):
                    raise ValueError
            except (json.JSONDecodeError, ValueError):
                raise HTTPException(
                    status_code=400, detail="since 必须是向量时钟 JSON 对象"
                )
        else:
            since_vv = None
        return db.pull_since(conn, since_vv)

    @app.post(
        "/sync/batch",
        response_model=BatchResult,
        tags=["sync"],
        summary="推送批次：整批原子写入，重复批次幂等，载荷冲突整批回滚",
        responses={400: {"description": "批次校验失败，已整批回滚"}},
    )
    def sync_batch(body: BatchBody, conn: sqlite3.Connection = Depends(get_conn)):
        try:
            result = db.apply_batch(
                conn,
                [op.model_dump() for op in body.ops],
                [res.model_dump() for res in body.resolutions],
            )
        except db.BatchRejected as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return BatchResult(
            station_id=station_id,
            current_vv=db.status(conn)["version_vv"],
            **result,
        )

    @app.post(
        "/sync/run",
        response_model=SyncResult,
        tags=["sync"],
        summary="一键双向同步：多轮拉/推直到两站向量时钟一致（可安全重复调用）",
        responses={503: {"description": "对站不可达（仍处离线状态）"}},
    )
    async def sync_run(conn: sqlite3.Connection = Depends(get_conn)):
        if not peer_url:
            raise HTTPException(status_code=503, detail="未配置 PEER_URL，无法发起同步")

        totals = {
            "pulled_ops": 0,
            "pulled_resolutions": 0,
            "pushed_ops": 0,
            "pushed_resolutions": 0,
        }
        rounds = 0
        try:
            async with httpx.AsyncClient(
                base_url=peer_url,
                timeout=10.0,
                transport=app.state.peer_transport,
            ) as client:
                for rounds in range(1, MAX_SYNC_ROUNDS + 1):
                    before_vv = db.status(conn)["version_vv"]

                    # 1) 拉：把对站因果未来的日志取回并整批应用
                    peer_status = (
                        await client.get("/status")
                    ).raise_for_status().json()
                    incoming = (
                        await client.get(
                            "/sync/pull",
                            params={"since": json.dumps(before_vv)},
                        )
                    ).raise_for_status().json()
                    if incoming["ops"] or incoming["resolutions"]:
                        applied = db.apply_batch(
                            conn, incoming["ops"], incoming["resolutions"]
                        )
                        totals["pulled_ops"] += applied["applied_ops"]
                        totals["pulled_resolutions"] += applied[
                            "applied_resolutions"
                        ]

                    # 2) 推：把本站相对对站时钟因果未来的日志发给对站
                    local_vv = db.status(conn)["version_vv"]
                    outgoing = db.pull_since(conn, peer_status["version_vv"])
                    if outgoing["ops"] or outgoing["resolutions"]:
                        resp = await client.post("/sync/batch", json=outgoing)
                        resp.raise_for_status()
                        totals["pushed_ops"] += len(outgoing["ops"])
                        totals["pushed_resolutions"] += len(
                            outgoing["resolutions"]
                        )

                    after_vv = db.status(conn)["version_vv"]
                    nothing_moved = not (
                        incoming["ops"]
                        or incoming["resolutions"]
                        or outgoing["ops"]
                        or outgoing["resolutions"]
                    )
                    if (
                        nothing_moved
                        and vv_equal(before_vv, after_vv)
                        and vv_equal(after_vv, peer_status["version_vv"])
                    ):
                        break
                else:
                    raise HTTPException(
                        status_code=500,
                        detail=f"同步 {MAX_SYNC_ROUNDS} 轮后仍未收敛，请重试（幂等安全）",
                    )
        except httpx.HTTPError as exc:
            raise HTTPException(
                status_code=503,
                detail=(
                    f"对站 {peer_url} 不可达，同步中止（本地数据未受影响）: {exc}"
                ),
            )

        return SyncResult(
            station_id=station_id,
            rounds=rounds,
            current_vv=db.status(conn)["version_vv"],
            **totals,
        )

    return app


# uvicorn app.main:app 使用环境变量装配的默认应用
app = create_app(
    station_id=os.environ.get("STATION_ID", "alpha"),
    db_path=os.environ.get("DB_PATH", "/data/station.db"),
    csv_path=os.environ.get("CSV_PATH", "/data/seed/sample_counts.csv"),
    peer_url=os.environ.get("PEER_URL", ""),
)
