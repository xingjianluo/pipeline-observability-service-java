# 流水线运行可观测服务（Pipeline Observability Service）

接收各条流水线跑批任务的运行事件，推算每条流水线当前状态，在出问题时发出区分类型的告警，
并把每一次判定的依据留痕，便于事后追溯。

## 1. 能力概览

- **事件接入**：支持 `STARTED / RETRYING / SUCCEEDED / FAILED / TIMED_OUT`。
  同一运行实例的事件允许**重复、乱序、迟到**，均不影响最终判定；
  非法或字段缺失的事件按**不同原因明确拒绝**（HTTP 400 + 原因码），不悄悄丢弃。
- **状态推算**：最近一次成功时间、连续失败次数、累计成功/失败数、
  是否超过新鲜度阈值、是否超过 SLA、是否达到连续失败阈值。
- **持久化与并发**：状态、告警抑制状态、配置、判定记录、告警历史均落盘（JSON，原子写），
  重启后结论一致；每条流水线一把锁，并发写同一条流水线不丢更新。
- **告警**：`CONSECUTIVE_FAILURES`（连续失败）、`SLA_BREACHED`（延迟完成/运行超时）、
  `STALE_NO_SUCCESS`（长时间无成功）三类严格区分；抑制窗口内同类只提醒一次，
  问题恢复后可重新触发。
- **审计**：每个事件的输入、判定 outcome、推算依据、触发的告警全部记录，可按流水线查询。

## 2. 事件模型

`POST /api/events`，请求体为 JSON：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `pipelineName` | 是 | 流水线名称 |
| `runId` | 是 | 同一运行实例的标识 |
| `type` | 是 | `STARTED` / `RETRYING` / `SUCCEEDED` / `FAILED` / `TIMED_OUT` |
| `eventTime` | 是 | ISO-8601 UTC 时间（如 `2026-09-29T10:00:00Z`），不得晚于接入时刻 |
| `detail` | 条件必填 | `FAILED` / `TIMED_OUT` 必须带失败原因 |
| `eventId` | 否 | 调用方去重标识；不填时用 `(type,eventTime)` 内容指纹去重 |

拒绝原因码（响应体 `rejectedReason`）：

| 原因码 | 含义 |
| --- | --- |
| `JSON_INVALID` | 请求体不是合法 JSON |
| `MISSING_PIPELINE_NAME` / `MISSING_RUN_ID` / `MISSING_EVENT_TYPE` / `MISSING_EVENT_TIME` | 缺字段 |
| `UNKNOWN_EVENT_TYPE` | 事件类型不认识 |
| `INVALID_EVENT_TIME` | 时间格式非法 |
| `EVENT_TIME_IN_FUTURE` | 事件时间晚于当前时间 |
| `MISSING_FAILURE_DETAIL` | 失败/超时事件没有 detail |

被拒绝的事件同样写入判定日志，`outcome=REJECTED:<原因码>`，绝不静默丢弃。

### 重复 / 乱序 / 迟到的处理规则

1. **幂等**：同一 `eventId`，或同一运行内 `(type,eventTime)` 完全相同，判定为 `DUPLICATE`，状态不变。
2. **结论只取决于事件集合**：每次写入后按业务事件时间归约运行状态，并按各运行的**完成时间**
   重新计算"最近成功/连续失败/累计数"。因此事件以任何顺序到达结论都一致。
3. **终态归约**：
   - `SUCCEEDED/FAILED/TIMED_OUT` 为终态；终态时间取终态事件中最早的一次；
   - 更早到达的终态事件可纠正乱序终态（例如先收到 FAILED，后补一条更早的 SUCCEEDED，最终算成功）；
   - 已终态后，迟到的 `STARTED/RETRYING`（事件时间早于已知终态）被忽略（`IGNORED_STALE`），
     不会把成功覆盖回运行中。
4. **连续失败计数**：只统计已终态运行，按完成时间从近到远回溯，遇到最近一次 SUCCEEDED 即停止。

## 3. 状态与新鲜度 / SLA 判定

`GET /api/pipelines/{name}/status` 返回：

- `lastSuccessAt`：最近一次成功运行的完成时间；
- `consecutiveFailures`：从最近一次完成运行回溯的连续失败（含超时）数；
- `totalSuccesses` / `totalFailures`：累计数；
- `stale`：`lastSuccessAt + freshnessThreshold <= now`（从未成功过不算 stale，
  该情况由连续失败告警覆盖）；
- `slaBreached`：取最近一个运行——已终态时 `endedAt > startedAt + sla`（延迟完成）；
  仍在运行时 `now > startedAt + sla`（运行中超时未完成）；
- `failureThresholdExceeded`：`consecutiveFailures >= failureAlertThreshold`；
- `activeAlertTypes`：当前仍活跃（未恢复）的告警类型。

阈值可通过 `POST /api/pipelines/config` 按流水线配置（不配置则用全局默认值）：

```json
{
  "pipelineName": "orders-nightly",
  "freshnessThreshold": "PT30M",
  "sla": "PT1H",
  "failureAlertThreshold": 3,
  "alertSuppressWindow": "PT15M"
}
```

## 4. 告警：抑制与恢复

每类告警（连续失败 / SLA / 新鲜度）各自维护独立状态：

1. 条件首次成立 → 立即告警，置 `active=true`，开启抑制窗口 `[firedAt, firedAt+window)`；
2. 窗口内条件持续成立 → **只提醒一次**，后续评估记为抑制；
3. 窗口过后问题仍在 → 允许再提醒一次（防止一次性告警被遗忘）；
4. 条件消失（成功使连续失败清零、新成功恢复新鲜度等）→ `active` 立即复位为 false，
   记一条"告警恢复"日志；此后再次出问题会**重新触发**，不受上一窗口影响。

即使没有新事件，时间流逝本身也可能产生告警（变 stale、运行中超时）。
这类"纯时间流逝型"条件只在两个地方用**真实时钟**评估：后台巡检
（`StalenessMonitor`，按 `observability.monitor-interval` 周期）和显式状态查询。
**事件接入路径不评估它们**——接入时以该事件的 `receivedAt` 为"现在"，
只判定由事件本身直接推出的条件（连续失败、延迟完成 SLA），
因此补发一条很久以前的历史事件，不会因为真实时钟已远晚于事件时间而误报 stale 或运行超时。
单测/集成测试可用 `observability.monitor-enabled=false` 关闭巡检。

告警历史：`GET /api/pipelines/{name}/alerts`。

## 5. 持久化与重启一致性

状态目录由 `observability.storage-dir` 指定，包含：

- `pipelines.json`：全部流水线聚合状态（含各运行的去重指纹）；
- `alerts.json`：各流水线各类型告警的抑制状态；
- `alert-history.json`：已发出的告警；
- `configs.json`：流水线阈值配置；
- `decisions.json`：追加式判定记录。

写入采用临时文件 + 原子替换。服务启动时整体加载，因此重启后：状态结论一致、
重复事件仍被识别、抑制窗口仍生效，不会重启后把问题当成新问题再报一遍。

并发方面，门面按流水线加锁，把"推算 → 告警 → 落盘"作为一个临界区，
不同流水线互不阻塞，同流水线并发写入被串行化，杜绝丢更新/状态覆盖。

## 6. 判定记录（事后追溯）

`GET /api/pipelines/{name}/decisions` 可查每条记录：

```json
{
  "pipelineName": "orders-nightly",
  "runId": "run-1",
  "eventType": "FAILED",
  "eventTime": "2026-09-29T10:00:20Z",
  "receivedAt": "2026-09-29T10:00:21Z",
  "outcome": "APPLIED",
  "reasons": [
    "新建运行实例 runId=run-1",
    "事件 FAILED @ 2026-09-29T10:00:20Z 生效",
    "聚合: lastSuccess=... consecutiveFailures=1 stale=false slaBreached=false failureThresholdExceeded=false"
  ],
  "firedAlerts": [],
  "decidedAt": "2026-09-29T10:00:21Z"
}
```

`outcome` 取值：`APPLIED`（生效）、`DUPLICATE`（重复，忽略）、
`IGNORED_STALE`（迟到旧消息，忽略）、`REJECTED:<原因码>`（接入拒绝）。

## 7. 本地验证

环境要求：JDK 21+、Maven 3.9+。

```bash
# 编译
mvn compile

# 跑全部单测/集成测试（16 个），重点覆盖：
#   EventOrderingTest            重复/乱序/迟到事件
#   EventRejectionTest           不同原因拒绝
#   ConcurrentIngestTest         16 线程并发写同一条流水线
#   RestartRecoveryTest          重启后状态恢复 + 重放去重
#   AlertSuppressionAndRecoveryTest 抑制/恢复重触发/三类告警区分/运行中超时
#   HistoricalEventSlaTest       历史事件不产生 SLA/stale 误报
#   AuditTrailTest               判定留痕（含拒绝事件）
#   EventApiIntegrationTest      HTTP 端到端
mvn test

# 启动服务
mvn spring-boot:run
```

手工打事件（另开终端）：

```bash
curl -s -XPOST localhost:8080/api/events -H 'Content-Type: application/json' -d '{
  "pipelineName":"demo","runId":"r1","type":"STARTED","eventTime":"2026-09-29T10:00:00Z"}'

curl -s -XPOST localhost:8080/api/events -H 'Content-Type: application/json' -d '{
  "pipelineName":"demo","runId":"r1","type":"FAILED",
  "eventTime":"2026-09-29T10:00:05Z","detail":"connection refused"}'

curl -s localhost:8080/api/pipelines/demo/status | python3 -m json.tool
curl -s localhost:8080/api/pipelines/demo/alerts  | python3 -m json.tool
curl -s localhost:8080/api/pipelines/demo/decisions | python3 -m json.tool
```

配置项（`src/main/resources/application.properties`，均有默认值）：

| 键 | 出厂默认 | 本仓库 properties 的值 | 说明 |
| --- | --- | --- | --- |
| `observability.storage-dir` | `data/observability` | 临时目录（随机） | 状态文件目录 |
| `observability.default-freshness-threshold` | 30min | 30s | 新鲜度阈值 |
| `observability.default-sla` | 60min | 60s | SLA |
| `observability.default-failure-alert-threshold` | 3 | 3 | 连续失败告警阈值 |
| `observability.default-suppress-window` | 15min | 15s | 告警抑制窗口 |
| `observability.monitor-enabled` | true | true | 是否开启后台巡检 |
| `observability.monitor-interval` | 30s | 30s | 巡检周期 |

> 出厂默认值见 `ObservabilityProperties`；仓库自带的 `application.properties`
> 把时间阈值调短，方便本地用 curl 在几十秒内观察到抑制、重触发等行为。

单测运行时会在日志中以 `>>> 输入事件 ...` 与 `<<< 判定 snapshot: ...`
打印输入事件与判定依据，判定记录也会逐条输出。
