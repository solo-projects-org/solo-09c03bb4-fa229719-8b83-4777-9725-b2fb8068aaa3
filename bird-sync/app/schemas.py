"""Pydantic 请求/响应模型（同时为 Swagger 提供示例）。"""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field

Vv = dict[str, int]


class CorrectBody(BaseModel):
    count: int = Field(..., ge=0, examples=[42], description="更正后的计数（非负整数）")
    expected_branch_vv: Optional[Vv] = Field(
        default=None,
        description=(
            "可选的乐观并发检查：提交者在读取时该记录上所见分支（冲突详情中的 branch_vv）。"
            "若读取后该分支已被覆盖（出现其因果后继），请求被拒绝（409）。"
        ),
    )


class ResolveBody(BaseModel):
    choice: Literal["count", "delete"] = Field(
        ..., description="裁定方式：count=选择某个计数分支；delete=删除"
    )
    count: Optional[int] = Field(
        default=None,
        ge=0,
        description="choice=count 时必填，且必须是当前某个冲突分支上的计数",
    )
    expected_branch_vv: Optional[Vv] = Field(
        default=None,
        description="可选乐观检查：读取冲突时某分支的向量时钟；读取后分支有变则拒绝（409）",
    )


class DeleteBody(BaseModel):
    expected_branch_vv: Optional[Vv] = Field(
        default=None, description="可选乐观检查：读取时所见分支的向量时钟"
    )


class Op(BaseModel):
    id: str = Field(..., description="操作全局唯一 ID，格式 <站点ID>-<站点内序列号>")
    record_id: str
    station_id: str
    seq: int = Field(..., ge=1)
    type: Literal["count", "delete"]
    count: Optional[int] = Field(None, ge=0)
    vv: Vv = Field(..., description="操作产生时刻的站点向量时钟")
    created_at: str


class Resolution(BaseModel):
    id: str
    record_id: str
    station_id: str
    choice: Literal["count", "delete"]
    count: Optional[int] = Field(None, ge=0)
    vv: Vv = Field(..., description="创建裁定时本站点的向量时钟快照")
    read_max_vv: list[Vv] = Field(..., description="读取冲突时所有极大分支的向量时钟")
    created_at: str


class BatchBody(BaseModel):
    ops: list[Op] = Field(default_factory=list)
    resolutions: list[Resolution] = Field(default_factory=list)


class BatchResult(BaseModel):
    station_id: str
    applied_ops: int
    applied_resolutions: int
    duplicate_ops: int
    duplicate_resolutions: int
    current_vv: Vv


class BranchOut(BaseModel):
    op_id: str
    station_id: str
    type: Literal["count", "delete"]
    count: Optional[int]
    vv: Vv
    created_at: str


class ConflictOut(BaseModel):
    record_id: str
    site: str
    species: str
    active: bool = Field(..., description="是否仍为待处理冲突（生效裁定后为 false）")
    branches: list[BranchOut]
    resolution: Optional[Resolution] = None
    resolution_effective: bool
    resolution_stale: bool


class RecordOut(BaseModel):
    record_id: str
    site: str
    species: str
    count: Optional[int] = Field(None, description="当前计数；记录被删除时为 null")
    deleted: bool
    conflicted: bool
    version_vv: Optional[Vv] = Field(
        None, description="当前生效分支（操作）的向量时钟；种子未改动记录为 null"
    )
    has_resolution: bool


class AnalysisRow(BaseModel):
    record_id: str
    site: str
    species: str
    count: int


class StatusOut(BaseModel):
    station_id: str
    version_vv: Vv
    op_count: int
    resolution_count: int
    record_count: int
    conflict_count: int


class SyncResult(BaseModel):
    station_id: str
    rounds: int
    pulled_ops: int
    pulled_resolutions: int
    pushed_ops: int
    pushed_resolutions: int
    current_vv: Vv


class MessageOut(BaseModel):
    detail: str
