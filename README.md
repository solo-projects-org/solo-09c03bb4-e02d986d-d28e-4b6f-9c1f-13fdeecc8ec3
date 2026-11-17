# 冷链记录仪校时平台

单台冷链温度记录仪的校时网页：记录仪断电重启后时钟失准，数据员上传原始 CSV
（开机段号、本地秒数、温度），再为每个开机段录入两枚「本地秒数 ↔ UTC」校时锚点，
系统对两锚点之间的读数做线性校时，质检组即可按 UTC 日期读取可信的温度原始点。

技术栈：FastAPI + SQLite（WAL），服务端渲染页面 + JSON API，后台线程异步重算。

## 快速启动（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

数据持久化在命名卷 `coldchain-data`（容器内 `/data/app.db`）。

本地开发（无需 Docker）：

```bash
pip install -r requirements-dev.txt
uvicorn app.main:app --reload        # http://localhost:8000
pytest -q                            # 运行自测
```

## 使用流程

1. **上传数据**（`/upload`）：CSV 三列 `segment,local_seconds,temperature`
   （支持中文表头 `开机段号,本地秒数,温度`，也支持无表头按此顺序）。
   - 累计最多 **2000** 点，超限整批拒绝；
   - 同一开机段内本地秒数不能重复（文件内或与已有数据），冲突整批拒绝；
   - 每批分配批次号，行号为 CSV 数据行序号（从 1 起）。
2. **校时锚点**（`/anchors`）：每段录入两枚锚点（本地秒数 1 < 本地秒数 2，
   且 UTC 必须递增）。两锚点之间的读数按线性插值校时；范围外或无锚点的读数
   **留存待校时**。**更正锚点必须提交所见版本**（乐观并发，版本不符返回 409）。
3. **按日查询**（`/query`）：按 UTC 日期查看已校时读数，每行显示
   **批次号、原行号、锚点版本**、校时版本；页面下方列出待校时读数及原因。
4. **逐版变更**（`/changes`）：数据员输入下游仓库**已同步的校时版本号**，取得此后
   每一已发布版本的**撤销（retract）与新增（add）记录**，用于下游增量同步
   （见下文「下游同步」）。
5. **处理状态**（`/`）：点数、重算状态（空闲/重算中/失败）、校时版本、
   数据代次、锚点与批次列表，可手动触发重算。

## 一致性设计

- **原子换版**：每次重算结果在单个事务内整体替换。读数跨日时，
  「旧日移除 + 新日加入」同事务生效；计算或提交失败时旧版本原样保留，
  **绝不提供半版**。
- **迟到计算不回退**：上传/锚点变更递增 `state_generation`；重算按一致快照
  计算，提交前复核代次，已过期则丢弃重算。校时版本号单调递增。
- **重算期间可写可读**：重算在后台线程进行，期间可继续上传新点，
  查询始终看到完整的旧版本；新点在随后的换版中生效。
- **重启恢复**：代次持久化于 SQLite，服务重启后自动比对并补算——
  所有已接纳且处于锚点范围内的点最终都会进入当前版。

## 下游同步（逐版变更）

下游仓库按 UTC 日保存已校时的冷链读数。锚点更正后，旧日期的点不能留在下游仓库，
因此每一已发布校时版本都同时发布**相对上一版的逐条变更**：

- `retract`：撤回旧记录。其 `utc_date`/`utc_time`/`anchor_version` 是**被撤回的旧值**，
  下游据此从对应旧日桶删除；读数跨日、退出锚点范围、UTC 时刻或锚点版本变化都会产生
  撤销（变化同时产生一条对应的 `add`，下游写入新日桶），旧日不会残留失效点。
- `add`：新增或替换后的记录，含新 UTC 日与锚点版本。
- 每条变更含**稳定读数 ID**（`readings.id`，不随换版变化）、**批次号**、**原行号**、
  **UTC 时刻**、**UTC 日**、**锚点版本**、段号/本地秒数/温度与 `reason`。

一致性约定：

- **随校时结果一同发布**：`calibrated`、`calc_versions`、`calc_changes` 与版本号在同一
  事务内写入；计算或提交失败时整体回滚，旧版本及其变更完整保留，**不会留下半组变更**。
- **按版本重复领取**：`GET /api/changes?since=N` 总是返回 N 之后的所有已发布版本，
  同一请求可反复重放且结果一致；**零变更的版本也会出现**（`changes: []`），
  下游凭 `latest_version` 推进游标即可接续，不会越过空版本。
- **未重算完成时的覆盖范围**：响应中 `dirty=true`、`covers="published_versions_only"`，
  表示只覆盖已发布版本；上传或更正锚点后的变更在重算发布前不会出现在结果里，
  重算完成后用同一 `since` 重复领取即可接续。`/query` 返回同样的 `covers` 标注。
- `since` 超过已发布版本 → 409 `UNKNOWN_VERSION`（附带 `current_version`）；
  负数或非整数 → 400。旧库升级时，启动会把已有结果登记为版本（`reason: "adopted"`），
  下游从 `since=0` 领取即可获得与按日查询一致的全量状态。

## JSON API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/uploads` | 上传 CSV（multipart `file`），返回 `{batch_id, accepted}` |
| POST | `/api/anchors` | 录入/更正锚点 `{segment, local1, utc1, local2, utc2, version?}` |
| GET | `/api/readings?date=YYYY-MM-DD` | 按 UTC 日查询已校时读数 |
| GET | `/api/pending` | 待校时读数及原因 |
| GET | `/api/versions` | 已发布校时版本列表（含新增/撤销条数） |
| GET | `/api/changes?since=N` | N 之后逐版撤销/新增记录（含零变更版本） |
| GET | `/api/status` | 点数、重算状态、代次、锚点、批次 |
| POST | `/api/recalc` | 手动触发重算 |

`/api/changes` 响应示例：

```json
{
  "since_version": 3,
  "latest_version": 5,
  "calc_version": 5,
  "dirty": false,
  "status": "idle",
  "covers": "all_changes",
  "versions": [
    {"version": 4, "created_at": "…", "state_generation": 7,
     "additions": 1, "retractions": 1,
     "changes": [
       {"action": "retract", "reason": "date_changed+utc_changed+anchor_version_changed",
        "reading_id": 12, "batch_id": 2, "row_no": 5, "segment": 1,
        "local_seconds": 150, "temperature": 4.2,
        "utc_time": "2026-01-01T23:30:00.000Z", "utc_date": "2026-01-01",
        "anchor_version": 1},
       {"action": "add", "reason": "date_changed+utc_changed+anchor_version_changed",
        "reading_id": 12, "batch_id": 2, "row_no": 5, "segment": 1,
        "local_seconds": 150, "temperature": 4.2,
        "utc_time": "2026-01-02T23:30:00.000Z", "utc_date": "2026-01-02",
        "anchor_version": 2}
     ]},
    {"version": 5, "additions": 0, "retractions": 0, "changes": []}
  ]
}
```

下游应用顺序：同一版本内先应用全部 `retract` 再应用 `add`（响应已按此排序），
按版本号递增逐版应用；成功后把游标置为 `latest_version`。

错误返回 `{detail, code, errors?, current_version?}`；锚点版本冲突为 409。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `DB_PATH` | `data/app.db`（容器内 `/data/app.db`） | SQLite 路径 |
| `MAX_POINTS` | `2000` | 累计点数上限 |
| `WORKER_ENABLED` | `1` | `0` 时关闭后台线程，变更后同步重算（测试用） |
| `RECALC_DELAY` | `0` | 重算提交前人为延迟秒数（演示/测试并发窗口） |

## 已知边界

- 面向单台记录仪、≤2000 点的场景，全量重算代价可忽略，未做增量计算。
- 校时结果毫秒精度；锚点区间按闭区间处理。
- 变更账本随版本永久保留（每版最多 2×2000 条），未做清理/分页；适用于单台记录仪。
- 未实现鉴权与多用户，部署在内网/受信环境。
