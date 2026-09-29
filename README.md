# 流水线运行可观测服务（Pipeline Observability Service）

接收外部各条流水线跑批任务上报的**运行事件**，推算每条流水线当前状态，在出问题时发出**可区分类型**的告警，并且全部判定依据可落盘、可重启恢复、可事后追溯。

---

## 1. 快速开始

```bash
# 构建与测试
mvn spring-boot:run          # 启动（端口默认随机，可在 application.properties 固定 server.port）
mvn test                     # 运行全部单测（18 个）
```

数据默认落盘在 `data/observability/`：

```
data/observability/
├── pipelines/<pipelineName>.json   # 每条流水线一份状态（原子写）
└── rejected-events.jsonl           # 被拒事件逐条留痕
```

### 接入一条事件

```bash
curl -s -X POST http://localhost:8080/api/events \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"e-1001","pipelineName":"daily-etl","runId":"run-20260929-01",
       "eventType":"STARTED","eventTime":1780100000000}'
```

成功返回 `{"accepted":true,"duplicate":false,...}`；重复事件返回 `duplicate:true`；
非法事件返回 **HTTP 422** 并带具体 `rejectReason`，同时写入 rejected 留痕。

### 查询

```bash
curl -s http://localhost:8080/api/pipelines                       # 全部流水线派生状态
curl -s http://localhost:8080/api/pipelines/daily-etl/status      # 单条状态
curl -s http://localhost:8080/api/events/rejected                 # 被拒事件（含原因与原始载荷）
```

---

## 2. 事件模型

| 字段 | 必填 | 说明 |
|---|---|---|
| `eventId` | 是 | 事件唯一 ID，**在整条流水线（跨 run）范围内做幂等去重** |
| `pipelineName` | 是 | 流水线名 |
| `runId` | 是 | 一次运行实例 ID |
| `eventType` | 是 | `STARTED` / `SUCCESS` / `FAILED` / `RETRIED` / `TIMED_OUT` |
| `eventTime` | 是 | 事件发生时间，**epoch 毫秒**，正整数。一切判定以它为准，与到达顺序无关 |

### 2.1 拒绝原因（不静默丢弃，按原因区分）

| rejectReason | 触发条件 |
|---|---|
| `MALFORMED_JSON` | 请求体不是合法 JSON 对象 |
| `MISSING_EVENT_ID` / `MISSING_PIPELINE_NAME` / `MISSING_RUN_ID` | 对应必填字段缺失或为空白 |
| `MISSING_EVENT_TYPE` | eventType 缺失或为空白 |
| `MISSING_EVENT_TIME` | eventTime 缺失（含非数字类型） |
| `UNKNOWN_EVENT_TYPE` | eventType 不在五种取值内 |
| `INVALID_EVENT_TIME` | eventTime 存在但不是正数 |

被拒事件写入 `rejected-events.jsonl`（含原因、可解析字段、原始载荷、拒绝时间），可经 `/api/events/rejected` 查询。

### 2.2 重复 / 乱序 / 迟到的归并规则

所有判定都基于"已见事件集合"，因此**任意到达顺序、任意重复次数，最终状态相同**。

- **去重**：`eventId` 已见过则直接忽略，状态不变（幂等）。
- **STARTED**：`startedAt` 取所有 STARTED 事件中最小 `eventTime`——迟到的 STARTED 也能修正开始时间（影响 SLA）。
- **RETRIED**：每接受一条不同的 RETRIED 事件，该 run 的尝试次数 +1。
- **终态（SUCCESS/FAILED/TIMED_OUT）**：
  - 以 **`eventTime` 较晚者为准**：迟到的"更晚终态"可覆盖早先结论；迟到的"更早终态"不会推翻新结论；
  - `eventTime` 相同时按优先级打破平局：**TIMED_OUT > FAILED > SUCCESS**。

---

## 3. 状态与新鲜度 / SLA 判定

状态（`PipelineDerivedStatus`）是由落盘的原始归并结果 + 阈值 + 当前时间**纯函数派生**的视图，不依赖任何进程内可变计数，因此重启后重新派生的结论与重启前完全一致。

- **lastSuccessAt**：所有成功终态 run 的 `finishedAt` 最大值。
- **consecutiveFailures**：终态 run 按 `finishedAt` 倒序，从最近一次往前连续为 FAILED/TIMED_OUT 的个数（遇到 SUCCESS 中断）。
- **新鲜度（freshnessExceeded）**：
  - 有过成功：`now - lastSuccessAt > freshnessThresholdMs`；
  - 从未成功：以**最早活动时间**（最早 STARTED 或终态的最小时间）为基准，超过阈值才算 stale，避免刚启动、第一次失败就误报"长时间没成功"；
  - 完全没有事件的流水线不算 stale。
- **SLA（slaExceededOnLastRun）**：最近一次终态 run 的 `finishedAt - startedAt > slaThresholdMs`（成功或失败都可能延迟完成）。

### 阈值配置（`application.properties`，均可覆盖）

| 配置 | 默认 | 含义 |
|---|---|---|
| `pipeline.observability.freshness-threshold-ms` | 3600000 | 新鲜度阈值 1h |
| `pipeline.observability.sla-threshold-ms` | 1800000 | SLA 阈值 30min |
| `pipeline.observability.consecutive-failure-threshold` | 3 | 连续失败告警阈值 |
| `pipeline.observability.alert-suppression-window-ms` | 1800000 | 告警抑制窗口 30min |
| `pipeline.observability.scan-interval-ms` | 60000 | 定时巡检间隔（驱动 stale 告警） |
| `pipeline.observability.storage-dir` | data/observability | 持久化目录 |

### 持久化与并发

- 每条流水线一个 JSON 文件，写入走"临时文件 + 原子 move"，崩溃也不会产生半截状态。
- **每条流水线一把锁**：去重判断 → 事件归并 → 告警状态更新在同一次锁内原子完成；并发写同一条流水线不丢更新、不互相覆盖。
- 只读派生也在同一把锁内取不可变快照，避免与并发写同时遍历可变状态。
- 启动时从磁盘全量加载（含告警抑制状态、被拒事件），重启前后结论一致。

---

## 4. 告警：抑制与恢复

三类问题**严格区分**，绝不合并为笼统失败：

| AlertType | 触发 | 作用域 | 恢复条件 |
|---|---|---|---|
| `CONSECUTIVE_FAILURES` | 连续失败/超时次数 ≥ 阈值 | pipeline | 出现一次成功 |
| `DELAYED_COMPLETION` | 某 run 的 STARTED→终态耗时 > SLA（成功但很慢也算） | **run** | 单次 run 的既成事实，按 run 各提醒一次 |
| `STALE_NO_SUCCESS` | 超新鲜度阈值仍无成功 | pipeline | 重新出现成功 |

- **抑制窗口**：同一作用域内同一问题处于激活态时，在 `alert-suppression-window-ms` 内只提醒一次；超过窗口且问题仍持续，会再发一次"心跳式"提醒。
- **恢复后重触发**：问题恢复时激活态翻为 false 并记录恢复时间；下一次再达到触发条件会作为**新的问题周期重新提醒**。
- 抑制 / 触发 / 恢复都会写日志（`[alert] FIRE / SUPPRESS / RECOVER`，含判定依据 basis），告警状态随流水线状态一起落盘，**抑制窗口跨重启依然有效**。
- `STALE_NO_SUCCESS` 由定时巡检（时间驱动）发现；另外两类在事件接入时即时评估。

---

## 5. 测试

```bash
mvn test
# 单个类
mvn test -Dtest=EventOrderingTest
```

| 测试类 | 覆盖重点 |
|---|---|
| `EventOrderingTest` | 重复幂等、乱序/迟到事件、终态覆盖规则、同时间优先级、迟到 STARTED 修正 SLA；同批事件不同顺序最终状态一致 |
| `EventValidationTest` | 8 种拒绝原因逐一覆盖、被拒事件留痕（不丢弃）、正常事件接受 |
| `ConcurrentWriteTest` | 32 线程并发写同一流水线不丢更新；同一 eventId 并发投递只接受一次 |
| `RestartRecoveryTest` | 重启后派生状态/事件/被拒记录恢复；告警抑制跨重启延续、恢复后重触发 |
| `AlertSuppressionTest` | 三类告警可区分、抑制窗口内不重复、越窗心跳、恢复后重触发、stale 巡检 |

测试日志会逐条打印**输入事件**（`[输入事件]`）与**判定依据**（`[判定依据]`，含 lastSuccessAt、连续失败数、新鲜度/SLA 布尔值、各类告警条数）。

### 本地手工验证示例

```bash
mvn spring-boot:run
# 1) 制造三次连续失败 -> 第 3 次出现 CONSECUTIVE_FAILURES，之后失败被抑制
# 2) 等待或调大时间制造新鲜度超限 -> 巡检产生 STALE_NO_SUCCESS
# 3) 上报一次成功 -> 连续失败与 stale 告警恢复
# 4) 重启服务后 GET 状态，结论不变；重复 POST 旧事件返回 duplicate:true
```
