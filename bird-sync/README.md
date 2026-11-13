# 候鸟样点计数 · 双站离线同步 API

两个离线营地（`alpha` / `bravo`，另附第三个演示营地 `charlie`）对同一份候鸟样点计数
CSV 进行**计数更正与删除**；断网时各自独立工作，联网后通过 API 双向同步，
自动检测冲突、支持人工裁定，为种群分析提供只含**无冲突且未删除**计数的取数接口。

## 1. 快速开始

```bash
docker compose up -d --build

# Swagger 入口（两个独立站点、各自独立数据卷）
#   alpha: http://localhost:8001/docs
#   bravo: http://localhost:8002/docs
#   charlie(可选第三营地): http://localhost:8003/docs

# 脚本演示：离线更正 → 同步 → 冲突 → 裁定 → 再同步 → 分析取数
bash scripts/demo.sh
```

停止与清理（数据保留在命名卷中；`-v` 连数据卷一起删除）：

```bash
docker compose down        # 保留数据，重启后修改/删除/冲突都在
docker compose down -v     # 清空数据卷，下次启动重新以 CSV 初始化
```

## 2. 数据模型与同步协议

### 2.1 存储（每站一个独立 SQLite 数据卷）

| 表 | 作用 |
| --- | --- |
| `records` | 当前对账结果：计数、删除标记、冲突标记、生效分支向量时钟 |
| `ops` | **只追加**的更正/删除日志，每条带站点向量时钟（version vector） |
| `resolutions` | 只追加的裁定日志，含裁定时读到的极大分支快照 `read_max_vv` |
| `meta` | 本站 ID、本地操作序号、裁定序号、当前向量时钟 |

种子文件 `data/seed/sample_counts.csv`（两站同一份），列：
`record_id,site,species,count`，其中 `count` 必须是非负整数；只允许改计数或删除。

### 2.2 向量时钟与因果关系

* 本地每产生一条更正/删除，本站分量 `+1`（如 `{"alpha":1}`）；
* 裁定推进专用分量 `"<station>:r"`（如 `{"alpha":1,"alpha:r":1}`），
  使裁定本身也能被增量同步识别；
* 接收批次**只并入、不递增**本站时钟，因此同一条操作在两站的向量时钟完全相同；
* 向量时钟 `a < b` 表示 `b` 因果包含 `a`（后者读到过前者）；互不包含即为**并发**。

### 2.3 冲突判定

对每条记录，取操作日志中没有因果后继的**极大操作集合**：

* 仅一个极大分支 → 无冲突（后继更正自动覆盖旧值/旧裁定）；
* 多个极大分支但**全删除**或**同值计数** → 自动合并，无冲突；
* 多个极大分支且计数不同，或删除与改值并存 → **列为冲突**，从分析取数中排除。

### 2.4 裁定语义

* `POST /conflicts/{record_id}/resolve`，`choice=count`（必须选当前某个分支上的计数）
  或 `choice=delete`（当前分支中必须存在删除操作）；
* 裁定记录读取时的极大分支集合 `read_max_vv`：
  * **读取后分支有变**（带 `expected_branch_vv` 乐观检查）→ 拒绝旧裁定（409）；
  * **裁定后才收到未见的并发更正**（极大分支集合变化）→ 裁定标记为陈旧，
    记录**重新列为冲突**，等待重新裁定；
  * 收到的是合并所有旧分支的**因果后继更正** → 静默覆盖旧裁定，不产生冲突；
* 两站各自裁定同一冲突时，按「因果优先 → 时间 → 裁定 ID」确定性择优，
  保证两站收敛到同一条生效裁定（裁定日志仍完整只追加保留）。

### 2.5 同步语义（可重复、乱序、中断重试）

* `GET /sync/pull?since=<向量时钟JSON>`：返回严格处于 `since` 因果未来的操作与裁定；
* `POST /sync/batch`：整批在一个 `BEGIN IMMEDIATE` 事务内写入，
  任一载荷非法或与同 ID 历史载荷不一致 → **整批回滚**；
* 按操作/裁定全局 ID **幂等去重**，重复、乱序、分批投递结果完全一致；
* `POST /sync/run`：一键多轮拉/推直到两站向量时钟相等，可安全重复调用；
  对站不可达返回 503，本地数据不受影响；
* 收齐相同更正、删除与裁定日志的两站，重算得到**相同的记录状态与冲突集合**。

## 3. HTTP 接口一览

| 方法 & 路径 | 说明 |
| --- | --- |
| `GET /records` / `GET /records/{id}` | 记录列表/详情（含 `deleted`、`conflicted`、`version_vv`） |
| `POST /records/{id}/corrections` | 更正计数，body `{"count": 非负整数, "expected_branch_vv"?: ...}` |
| `DELETE /records/{id}` | 删除（墓碑），可选 body 携带乐观检查 |
| `GET /conflicts` | 全部冲突及分支、裁定生效/陈旧状态 |
| `POST /conflicts/{id}/resolve` | 裁定：`{"choice":"count","count":N}` 或 `{"choice":"delete"}` |
| `GET /analysis/counts` | **种群分析取数**：仅无冲突且未删除的计数 |
| `GET /sync/pull?since=...` | 增量拉取操作与裁定 |
| `POST /sync/batch` | 推送批次（原子、幂等） |
| `POST /sync/run` | 一键双向同步 |
| `GET /status` / `GET /healthz` | 向量时钟与计数 / 健康检查 |

## 4. 典型手工流程（curl）

```bash
A=http://localhost:8001; B=http://localhost:8002

# 离线各自更正（产生并发）
curl -s -X POST $A/records/R001/corrections -H 'Content-Type: application/json' -d '{"count":50}'
curl -s -X POST $B/records/R001/corrections -H 'Content-Type: application/json' -d '{"count":70}'

# 联网同步并查看冲突
curl -s -X POST $A/sync/run
curl -s $A/conflicts

# 裁定取 70，再同步，对站收敛
curl -s -X POST $A/conflicts/R001/resolve -H 'Content-Type: application/json' -d '{"choice":"count","count":70}'
curl -s -X POST $A/sync/run
curl -s $B/records/R001
```

乐观并发检查：先从 `GET /conflicts`（或记录详情的 `version_vv`）读取分支向量时钟，
提交时放入 `expected_branch_vv`；读取后该分支若已被后继覆盖，接口返回 409。

## 5. 本地开发与测试

```bash
python3 -m venv --without-pip .venv          # 无 pip 的环境可先 bootstrap
.venv/bin/python -m pip install -r requirements.txt httpx pytest
.venv/bin/python -m pytest tests/ -q         # 28 个端到端用例（进程内双站/三站）
```

测试覆盖：种子一致性、并发计数/删除冲突、同值与双删自动合并、因果链无冲突、
裁定计数/删除、陈旧裁定拒绝、未见并发更正重开冲突、后继更正静默覆盖、
乐观锁、批次原子回滚、幂等/乱序/中断重试收敛、重启持久化、卷站点身份保护、
双站并发裁定确定性收敛。

## 6. 目录结构

```
bird-sync/
├── app/
│   ├── main.py        # FastAPI 应用工厂与全部 HTTP 路由
│   ├── db.py          # SQLite 持久化、操作/裁定日志、对账与冲突检测
│   ├── vv.py          # 向量时钟
│   └── schemas.py     # 请求/响应模型（Swagger 示例）
├── data/seed/sample_counts.csv
├── scripts/demo.sh
├── tests/test_sync.py
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```
