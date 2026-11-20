# 采集编排器（Collection Orchestrator）

轻量、可持久化的采集流水线编排器：节点组成 DAG，支持并行分支、条件跳过、失败重试与人工暂停。页面展示 DAG、节点日志摘要、重试范围与恢复入口。

零第三方依赖：Node.js 内置 `http` + `node:sqlite`，前端为原生 ES Module。

## 快速开始

```bash
node server.js          # http://localhost:4173
npm test                # node --test，49 个用例
```

可选环境变量：`PORT`（默认 4173）、`DATA_DIR`（运行状态目录）、`SOURCE_DIR`（夹具数据源）、`CONCURRENCY`（并行节点数，默认 4）。

页面左侧的“运行夹具”可直接创建运行；`fixtures/graphs/` 下每个文件都是一条流水线定义：

| 夹具 | 用途 |
| --- | --- |
| `orders-daily.json` | 下载 → 并行解析/校验 → 发布 → 汇总，含条件跳过与 `pauseAfter` 人工暂停点 |
| `cycle.json` | 循环依赖，编译期拒绝并给出环路 |
| `partial-failure.json` | 一条分支成功、一条失败；join 节点与失败分支的下游被阻塞 |
| `resume.json` | 端到端恢复用的小流水线 |

## 持久化与副作用语义

运行状态全部落库（`<DATA_DIR>/orchestrator.db`），重启不重复执行已确认的副作用：

- **节点状态机**：`pending → running → succeeded | failed | skipped | blocked | cancelled`。状态迁移、日志、事件与副作用账本在同一事务性存储里；进程重启时只把 `running` 节点重新排队，绝不臆断其成功。
- **副作用账本**（`side_effects` 表）：执行器通过 `ctx.effect(key, meta, fn)` 写外部系统。键一旦确认，后续重试/恢复直接复用（`effect_adopted`），不会二次写入。`publish` 节点的键由“目标 + 上游校验摘要”决定。
- **完整输入**：节点只有在全部上游 `succeeded` 时才启动；任一上游 `failed`/`blocked`/`cancelled` 时节点记为 `blocked`，且不记录输入/输出。上游 `skipped` 时下游按保守语义 `skipped`，不会用空输入继续跑。
- **重试**：节点级 `retry.maxAttempts` + 指数退避；`err.retryable = false` 时立即失败。人工重试的范围是“该节点 + 全部下游”，范围会写进事件，页面对失败/阻塞节点展示该范围。手动重试复用已确认副作用。
- **人工暂停**：`pauseAfter: true` 的节点完成后运行进入 `paused` 并记录 `gate`；也可以在运行中手动暂停（在途节点先完成，不再调度新节点）。暂停状态跨重启保留，页面顶部横幅即恢复入口。
- **取消**：中断在途节点（AbortSignal），未开始节点标记 `cancelled`，运行终态为 `cancelled`；重启不会复活。

## 定义格式

```json
{
  "id": "orders-daily",
  "name": "订单日报采集",
  "nodes": [
    { "id": "download", "type": "download", "config": { "source": "orders" } },
    { "id": "parse_cny", "type": "parse", "dependsOn": ["download"],
      "retry": { "maxAttempts": 3, "backoffMs": 40, "backoffFactor": 2 } },
    { "id": "validate_cny", "type": "validate", "dependsOn": ["parse_cny"],
      "skipWhen": "parse_cny.output.count == 0" },
    { "id": "publish_cny", "type": "publish", "dependsOn": ["validate_cny"],
      "pauseAfter": true, "config": { "target": "cny-orders" } }
  ]
}
```

- `type`：`download | parse | validate | publish | custom`（执行器可在 `src/executors.js` 扩展）。
- `dependsOn`：上游节点；同一层的节点并行执行（受 `CONCURRENCY` 限制）。
- `skipWhen`：小型条件表达式，`&&` `||` `!` 括号，字段 `node.status` / `node.output.*` / `node.input.*` / `node.meta.*`，支持 `==`/`!=` 与字面量。编译期静态校验：只能引用祖先节点，环路、未知依赖、非法表达式都会在创建运行前以 422 拒绝。
- `retry`：`maxAttempts`（1–20）、`backoffMs`、`backoffFactor`、`maxBackoffMs`。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/fixtures` | 夹具列表及编译校验结果（循环依赖夹具会显示 `CYCLE` 与环路） |
| POST | `/api/runs` | `{fixture}`、`{definition}` 或 `{pipelineId}` 创建并启动运行 |
| GET | `/api/runs/:id` | 运行详情：节点、日志摘要、事件、副作用、失败节点重试范围 |
| GET | `/api/runs/:id/stream` | SSE：`snapshot` / `events` / `heartbeat` / `closed` |
| POST | `/api/runs/:id/pause`\|`resume`\|`cancel` | 人工控制；非法状态返回 409 |
| POST | `/api/runs/:id/retry` | `{nodeId}`，按“节点 + 下游”重跑 |
| GET | `/api/runs/:id/retry-scope?nodeId=` | 预览重试范围 |
| POST | `/api/trigger` | 写入文件触发器，由 `FileTrigger` 原子认领并派发（模拟外部调度） |

## 代码结构

```
server.js              入口：启动 app + 优雅关闭（等待在途节点落盘）
src/app.js             HTTP 路由、SSE、静态资源
src/engine.js          调度器：状态机、并行、重试、暂停/取消、恢复、副作用账本
src/graph.js           定义编译与校验：拓扑序、环路检测、条件语法/静态引用检查
src/store.js           SQLite schema 与查询（runs / node_runs / events / side_effects）
src/executors.js       夹具执行器（download/parse/validate/publish/custom）
src/trigger.js         文件触发器（rename 原子认领，崩溃安全）
public/                仪表盘（DAG、状态瓦片、节点日志摘要、事件流、操作按钮）
test/                  graph / engine / semantics / recovery / api 五组用例
```

## 测试覆盖的关键行为

- 循环依赖：编译期报 `CYCLE` 并给出环路；HTTP 返回 422 且不落库。
- 并行分支：同层节点确实重叠执行；`CONCURRENCY` 上限生效。
- 条件跳过：条件命中 → `skipped`；上游跳过 → 下游保守跳过；混合 join 不会用部分输入继续。
- 上游部分失败：失败分支下游与 join 全部 `blocked`，成功分支可独立发布完成。
- 重试：次数与退避符合配置；不可重试错误立即失败；预算耗尽展示最后一次错误。
- 取消：中断在途节点、未开始节点不再执行、重启不复活。
- 暂停：在途节点完成、不再调度、跨重启保留、恢复后从未完成处继续。
- 断点恢复：运行中节点重启后重新排队；副作用已确认但节点未确认的崩溃窗口只复用不重复写入（外部队列文件行数断言）。
- 人工重试：只重跑目标节点及下游，范围外节点不动，已确认副作用被复用。
