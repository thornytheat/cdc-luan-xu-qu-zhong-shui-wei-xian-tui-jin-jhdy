# cdcwater · CDC 乱序去重与水位线推进

面向数据集成组的 CDC 合并工具：把订单 / 客户 / 库存三库的变更事件（乱序、重复、迟到）
合并成可信的最终行，给出可复现的水位线，并在放行 schema 变更前逐条评估影响。

- 形态：单服务 FastAPI + Jinja2 服务端模板 + 原生 JS，SQLite 命名卷持久化。
- 端口：宿主机 `18213`（容器内 8000）。
- 样例：3 张表 × 60 条事件，故意埋 8 条迟到、3 条同版本冲突、2 条墓碑后迟到、1 条同内容重复。

## 能力

1. **事件归档**：JSONL 逐行解析、字段校验，事件保留「文件 + 行号」作为所有结论的定位依据；导入幂等（唯一键 source + line_no）。
2. **同键合并**：同 pk 保留最大 `version`；同版本同内容静默去重；同版本不同内容判 `conflict` 且不覆盖先到内容。
3. **墓碑处理**：`op=d` 且版本更高 → 键判删除，墓碑保留；墓碑后低版本 c/u 忽略并计入 `ignored_after_tombstone`；只有严格更高版本才复活。
4. **乱序与水位线**：按到达顺序判定迟到（`ts < 当前已处理最大 ts`），迟到事件按业务时间进入**待补队列**，不再直接改写已发布结果；`watermark = min(每个键已处理事件的最大 ts)`，输出全局/分表水位线、未闭合键数与推进轨迹。
5. **待补与发布切换**：补齐同一键边界前的事件后才切换；新版本最终结果与水位线在**同一事务**切换，失败保留旧版本；同一批重试复用原版本，不新增版本；墓碑与冲突事件全程保留。
6. **Schema 门禁**：对比 v1 → v2，逐条给「可放行 / 放行+回填 / 阻断」，附列位置与理由。
7. **快照导出**：每表最终行 + 墓碑 + 冲突明细 + 待补队列位置与原因 + 前后水位线 + 门禁结论 + 全部变更日志，JSON 可下载。

## 核心规则口径

| 规则 | 口径 |
|---|---|
| 最大版本 | 同 pk 只保留 `version` 最大的事件；冲突时保留**先到**内容 |
| 同版本 | `after` 按字段排序后逐字比较：一致 → 静默去重（不计冲突）；不一致 → 冲突（列出两条事件与差异字段） |
| 墓碑 | 删除事件写入后不可丢；`version < 墓碑版本` 的事件一律忽略，且不改状态 |
| 迟到 | 文件行序处理，`event.ts < 当前已处理最大 ts`（相等不算迟到），记录表名 + 行号 + 当时最大 ts |
| 水位线 | `min(每个键已处理事件的最大 ts)`；`键最大 ts <= 水位线` 为已闭合键，其余为未闭合键 |
| 待补入队 | 迟到事件只入队不改写；`ts <= 该键边界（key_max_ts）` → 水位线以内迟到，否则 → 到达顺序迟到；墓碑后迟到另记墓碑版本 |
| 补齐切换 | 选中事件的同键边界内事件必须同批；新版本与 `published_current` 指针同一事务提交，失败整体回滚 |
| 批次幂等 | `batch_key = hash(表 + 补齐后的事件集合 + 目标 schema)`；成功批次重试返回 `reused=true` 原版本，失败批次重试复用同一键 |
| 门禁 | 新增 nullable → `compatible`；新增带默认值 → `compatible_with_backfill`；删列 / 数值收窄 / 类型族不兼容 / 新增非空无默认 / 可空收紧为非空无默认 → `blocked`；总判定取最严 |

## 启动

```bash
cd batch-20261114b/projects/gb-624
cp .env.example .env            # 可选，默认端口 18213
docker compose up -d --build
docker compose ps               # 等 healthy
curl -s http://127.0.0.1:18213/health
```

打开 <http://127.0.0.1:18213/>。SQLite 落在命名卷 `cdcwater-data:/app/data`；
重建数据：`docker compose down -v` 后重新 `up`（启动时幂等 seed，重复启动不会重复导入事件）。

## 演示步骤

1. 首页看样例概览与埋点说明（8 迟到 / 3 冲突 / 2 墓碑后迟到 / 1 重复）。
2. 工作台勾选 3 张表、目标 schema 选 v2，点「执行合并」，看进度与即时摘要。
3. 结果页：每表合并行数、冲突逐条（两条事件行号 + 差异字段）、待补原因（水位线以内 / 到达顺序 / 墓碑后）、
   当前水位线与补前水位线、发布版本、门禁逐条结论、快照预览。
4. 待补队列页：按业务时间看队列；选 cutoff（或全部）补齐，页面给出批次键、补前/补后水位线、待补前后条数、每键前后最终结果、保留的墓碑与冲突；可勾选「模拟失败」验证回滚，失败批次重试不新增版本。
5. 结果页或复核页下载快照 JSON；复核页可按结论筛选变更日志。

## API / 页面

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查（含事件数与运行数） |
| GET | `/` `/workbench` `/result/{run_id}` `/review` `/backfill` | 5 个页面 |
| POST | `/api/merge` | `{"tables":["orders"],"schema_version":2,"mode":"published"}` → run_id + 摘要；`mode=direct` 为旧的全量直算口径（仅对照） |
| POST | `/api/backfill` | `{"table":"orders","cutoff":"2026-11-10T00:15:00Z","schema_version":2}`；`cutoff` 留空/`all` 表示全部；`batch_key` 重试原批次；`simulate_failure` 演练回滚 |
| GET | `/api/backfill/state` | 每表当前发布版本、待补队列（原因/边界/状态）、批次审计与版本历史 |
| GET | `/api/runs` `/api/runs/{run_id}` | 历史运行 / 单次运行详请 |
| GET | `/api/runs/{run_id}/snapshot` | 下载快照 JSON（attachment） |
| POST | `/api/seed` | 幂等重放样例（已归档事件不重复插入） |
| GET | `/api/samples` `/api/schemas/{1\|2}` | 样例概览 / schema 文档 |

curl 验收：

```bash
RUN=$(curl -s -X POST http://127.0.0.1:18213/api/merge -H 'Content-Type: application/json' \
  -d '{"tables":["orders","customers","inventory"],"schema_version":2}' | python -c 'import json,sys;print(json.load(sys.stdin)["run_id"])')
curl -s http://127.0.0.1:18213/api/runs/$RUN | python -m json.tool
curl -s -o /tmp/cdcwater-snapshot.json http://127.0.0.1:18213/api/runs/$RUN/snapshot
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:18213/api/merge \
  -H 'Content-Type: application/json' -d '{"tables":["ghost"],"schema_version":2}'   # 400

# 待补：先看状态 → 补齐 → 重试同一批（不新增版本）→ 演练失败（旧版保留）
curl -s http://127.0.0.1:18213/api/backfill/state | python -m json.tool
BATCH=$(curl -s -X POST http://127.0.0.1:18213/api/backfill -H 'Content-Type: application/json' \
  -d '{"table":"orders","cutoff":null,"schema_version":2}' | tee /tmp/bi.json | python -c 'import json,sys;print(json.load(sys.stdin)["batch_key"])')
curl -s -X POST http://127.0.0.1:18213/api/backfill -H 'Content-Type: application/json' \
  -d "{\"table\":\"orders\",\"batch_key\":\"$BATCH\"}" | python -c 'import json,sys;d=json.load(sys.stdin);print(d["reused"], d["version_no"])'  # True 2
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:18213/api/backfill \
  -H 'Content-Type: application/json' -d '{"table":"inventory","simulate_failure":true}'   # 400，旧版不变
```

## 测试

```bash
python -m pytest -q                               # 本地
docker compose run --rm app python -m pytest -q    # 容器内
```

- `tests/test_merge.py`：去重、同版本冲突/同内容、墓碑、墓碑后忽略、复活、迟到仍按版本合并、样例埋点对齐。
- `tests/test_watermark.py`：迟到判定与位置、水位线公式、未闭合键、跨表合并、推进轨迹单调性。
- `tests/test_schema_gate.py`：三类结论矩阵 + 样例 v1→v2 全量（3 可放行 / 1 回填 / 5 阻断）。
- `tests/test_api.py`：成功、400/404/422、端到端（样例 → 合并 → 结论 → 快照）。
- `tests/test_backfill.py`：首发只合并按时事件、队列按业务时间排序、补齐原子切换、同键边界整段纳入、
  失败保留旧版本与队列、成功/失败批次重试不新增版本、墓碑与冲突保留、新事件越过边界即时发布、页面与快照口径。

## 目录

```text
app/
├── main.py          # FastAPI 路由（薄）
├── settings.py      # 路径、表清单、文案映射
├── archive.py       # 模块1 事件归档（解析/校验/幂等导入）
├── merge.py         # 模块2 同键合并
├── tombstone.py     # 模块3 墓碑/版本判定表（纯函数）
├── watermark.py     # 模块4 迟到与水位线
├── schema_gate.py   # 模块5 schema 门禁
├── backfill.py      # 模块7 待补队列、版本发布与原子切换
├── snapshot.py      # 模块6 快照导出与页面预览
├── runner.py        # 编排 + 落库（published / direct 两种口径）
├── store.py         # SQLite（events / runs / published_* / pending_events / backfill_batches / meta）
├── templates/       # base/index/workbench/result/review/backfill/not_found
└── static/          # app.js / style.css（原生，无框架）
sample_data/
├── generate_samples.py   # 确定性生成器（末尾自检 8 条迟到）
├── events/{orders,customers,inventory}.jsonl
└── schemas/{v1,v2}.json
tests/                      # pytest
```

## 已知限制

- 事件只支持 JSONL 落盘后的批量合并，不做实时流接入；重复跑同一批事件会得到同一结果（幂等）。
- 冲突只判定「同版本不同内容」，不做字段级自动合并；裁决后需人工重放修正事件。
- 迟到事件不阻塞发布：入队后需在「待补队列」页（或 `POST /api/backfill`）显式补齐；
  同一键的整段待补事件会同批补入，若某键持续有新迟到事件，可按业务时间边界分批推进。
- 补传只支持单表单批原子切换；跨表一致性由各表独立版本指针保证，不做跨表两阶段提交。
- 类型门禁覆盖 string/int/long/decimal/timestamp/boolean 的收窄与族不兼容，不做列重命名识别。
- 队列/上游 connector、鉴权、多租户不在本演示范围。
