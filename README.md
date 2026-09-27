# 工控边缘网关数据流程（本地可运行）

一套**完全本地化**的工控边缘网关数据流：本地模拟设备产生读数 → 采集与去重/乱序处理 →
协议转换（点位映射 / 单位换算 / 版本固化）→ 下游投递（限流 / 有界缓存 / 可恢复重试）。
无外部服务、无真实凭据，仅依赖 Spring Boot、Jackson 与 JUnit。

## 1. 快速开始

```bash
# 构建并运行全部测试（15 个，覆盖断线重连、重复/乱序、下游阻塞、重启恢复、映射回放、异常校验、并发）
mvn test

# 端到端本地演示（模拟多设备、断线重连、重发乱序、异常数据、下游变慢/不可用、恢复排空）
mvn package -DskipTests
java -jar target/industrial-edge-gateway-java-1.0-SNAPSHOT.jar \
     --gateway.demo.enabled=true

# 想在演示里看到限流拒绝：
java -jar target/industrial-edge-gateway-java-1.0-SNAPSHOT.jar \
     --gateway.demo.enabled=true \
     --gateway.spool.capacity=10 \
     --gateway.rate.permits-per-second=0.0001 --gateway.rate.burst=1

# 仅启动 HTTP 服务（随机端口由 server.port 控制，下面固定 8080）
java -jar target/industrial-edge-gateway-java-1.0-SNAPSHOT.jar --server.port=8080
```

运行期数据默认写在 `./data/`（投递 WAL、已确认幂等键、原始读数留存），可用配置覆盖。

## 2. 模块与数据流

```
SimulatedDevice → DeviceConnector(连接/断线/重连回放)
        │ RawReading(deviceId, pointCode, value, unit, timestamp(UTC ms), sequence, quality, duplicate)
        ▼
IngestionService（按设备串行）
   1. 质量码校验  BAD_QUALITY
   2. 时间戳基准校验  TIMESTAMP_SKEW
   3. 映射版本查点位  UNKNOWN_POINT
   4. DeviceAggregate 序列号状态机：重复 / 迟到乱序 / 缺口缓冲 / 连续应用
        │ TelemetryEvent（统一指标、标准单位、事件时间/接收时间、mappingVersion、dedupKey）
        ▼
DeliveryService
   幂等去重 → 令牌桶限流 → 直投 / 有界 WAL 缓存 / 拒绝
        ▼                         （后台 worker 重试，重启恢复）
SlowDownstreamSink（本地模拟：可设置延迟、可用/不可用、幂等确认）
```

关键包：

| 包 | 类 | 职责 |
|---|---|---|
| `model` | `RawReading` / `TelemetryEvent` / `IngestResult` / `DeliveryReceipt` 等 | 协议无关数据模型与枚举 |
| `ingest` | `IngestionService` | 采集入口：异常校验、判定、计数、判定日志 |
| `ingest` | `DeviceAggregate` | 单设备序列号水位 / 去重 / 乱序缺口缓冲 / 最终值与累计 |
| `ingest` | `DeviceConnector` / `SimulatedDevice` | 连接模拟（断线留存、重连重发）与读数产生 |
| `transform` | `MappingRegistry` / `PointMapping` / `TelemetryTransformer` | 多版本点位映射与单位换算 |
| `delivery` | `DeliveryService` / `TokenBucketRateLimiter` / `SlowDownstreamSink` | 限流、投递、重试、幂等 |
| `persistence` | `Spool` / `FileSpool` / `InMemorySpool` / `RawLog` / `*KeyStore` | 有界 WAL 缓存、原始留存、已确认键 |
| `replay` | `ReplayService` | 指定映射版本的确定性回放 |
| `observability` | `GatewayMetrics` / `DecisionLog` | 可对账计数与判定链路日志 |
| `web` | `GatewayController` | 本地观测/操作接口 |

## 3. 接入与转换规则

### 3.1 接入

每台设备用 `deviceId` 标识，设备内**序列号 `sequence` 单调递增**；读数还带设备时间戳
（epoch 毫秒、UTC 基准）与质量码。连接状态通过 `IngestionService.updateConnection` 维护。

- **断线**：`DeviceConnector.disconnect()` 后读数留在连接器侧（模拟边缘留存），不上报；
- **重连**：`drainReplayOnReconnect()` 把断线期间读数以 `duplicate=true`（传输层重发标记）重放。
  是否真的重复**只由网关按序列号判定**——重放内容可能包含“网关已收到但 ACK 丢失”的读数，
  连接层不做会误事的“聪明去重”。

### 3.2 映射与单位换算（默认 v1）

| 设备 | 原始点位 | 原始单位 | 统一指标 | 标准单位 | 类型 | 换算 |
|---|---|---|---|---|---|---|
| device-A | `TEMP_c` | °C | `temperature_c` | °C | 瞬时 | `v` |
| device-A | `TOTAL_flow_l` | L | `total_volume_l` | L | **累计** | `v` |
| device-B | `temp_f` | °F | `temperature_c` | °C | 瞬时 | `(v-32)*5/9` |
| device-B | `flow_m3` | m³ | `total_volume_l` | L | **累计** | `v*1000` |

换算公式统一为 `标准值 = 原始值 * factor + offset`；点位类型（瞬时/累计）随规则保存。

### 3.3 时间基准

- `eventTime`：设备时间戳（UTC ms），始终保留原始值，不做静默改写；
- `ingestTime`：网关接收时间（UTC ms）；
- 若两者偏差超过 `gateway.max-clock-skew-millis`（默认 60s），读数判 `REJECTED/TIMESTAMP_SKEW`。

### 3.4 映射版本与回放

- 映射规则按**不可变版本**保存在 `MappingRegistry`，调整规则 = 发布新版本（`register`），
  已发布版本永不修改；最新注册版本为“当前版本”。
- 每条 `TelemetryEvent` 都固化 `mappingVersion`；同版本规则 + 相同原始输入 ⇒ 相同输出。
- 原始读数以 JSON-Lines 追加留存（`RawLog`，默认 `./data/raw/readings.log`）。
- `ReplayService` 用**指定版本**重新归一化，不写状态、不投递下游，因此回放是确定性纯计算；
  该版本下无映射的点位会以 `note=UNKNOWN_POINT in <version>` 占位，不会悄悄消失。
  HTTP：`GET /api/replay?version=v1`。

## 4. 异常数据处理语义（不把异常当正常数据吞掉）

采集侧每条输入落入且仅落入一个出口：

| 结果 | 触发条件 | 是否累计 | 是否投递 |
|---|---|---|---|
| `ACCEPTED` | 质量/时间/映射校验通过，且序列号被应用或进入缺口缓冲 | 应用时累计，每序号恰好一次 | 应用（或补齐连带应用）后投递 |
| `DUPLICATE` | 序列号已应用或已在缺口缓冲（含传输层重发） | 否 | 否 |
| `OUT_OF_ORDER` | 序列号落后于缺口起点且未见过（含 LRU 淘汰后的迟到重放） | 否 | 否 |
| `REJECTED` | `BAD_QUALITY` / `TIMESTAMP_SKEW` / `UNKNOWN_POINT` / `GAP_SEQUENCE`（缺口跨度/缓冲超限） | 否 | 否 |

乱序处理细节（`DeviceAggregate`，记 `firstMissing = 水位+1`）：

- `seq == firstMissing`：应用，并连带应用缺口中连续的后续序号（按序放行投递）；
- `firstMissing < seq` 且跨度 ≤ 缺口缓冲容量（默认 64/设备）：进缺口缓冲，**先不投递**；
- 跨度或容量超限：`REJECTED/GAP_SEQUENCE`；
- 累计型指标只在“应用”瞬间累加，因此**重复、迟到、乱序都不会造成重复累计或漏累计**；
- 去重集合是有界 LRU（默认 512/设备），淘汰后迟到的旧序号重放判 `OUT_OF_ORDER` 而非
  `DUPLICATE`——两者终态不同，但都不会二次累计。

所有判定通过 `DecisionLog` 同时写日志和内存（测试中用 `dump(deviceId)` 打印输入、判定依据、
水位/累计等状态变化）。

## 5. 背压、缓存与重复投递行为

`DeliveryService` 对每条采集成功事件给出**可区分**的终态/暂态：

| 状态 | 依据 | 语义 |
|---|---|---|
| `DELIVERED` | 有令牌且下游确认 | 终态成功 |
| `DUPLICATE_DELIVERY` | 幂等键已确认（内存集合 + 持久化键文件） | 重复投递可辨认，不打下游 |
| `BUFFERED` | 无令牌，或下游不可用/发送失败，且缓存未满 | 暂态：进有界 WAL，后台 worker 重投 |
| `RATE_LIMITED` | **限流**拿不到令牌且缓存已满 | 终态拒绝，原因可区分 |
| `DOWNSTREAM_DROPPED` | **下游明确不可用**且缓存已满 | 终态丢弃，原因可区分 |

- **上限明确**：缓存容量 `gateway.spool.capacity`（默认 1000）是拒绝的唯一硬上限；
  超限后根据“限流”还是“下游不可用”给出两种不同原因，绝不静默丢弃。
- **不丢**：缓存为 JSON-Lines WAL（`./data/spool/spool.log`），worker 每轮 poll 一批，
  仍不可投递的事件按原顺序**整体放回队首**（`prepend`），恢复后全部送达；进程重启后
  WAL 自动恢复。损坏行转入 `spool.dead-letter.log` 供人工核查，不阻塞其余数据。
- **不重**：幂等键为 `deviceId:sequence`；已确认键持久化在 `ack-keys.log`，重启后重复投递
  仍判 `DUPLICATE_DELIVERY`；下游模拟也按幂等键拒绝重复发送。
- 损坏/无法解析的 WAL 行不参与重试，单独留存在 dead-letter 文件。

## 6. 对账（采集成功 / 被拒绝 / 被丢弃数量对得上）

`GatewayMetrics` 维护恒等式（`GET /api/reconcile`，不满足直接抛错）：

```
输入总数  = ACCEPTED + DUPLICATE + OUT_OF_ORDER + REJECTED
ACCEPTED  = DELIVERED + DUPLICATE_DELIVERY + RATE_LIMITED + DOWNSTREAM_DROPPED
            + spoolSize(存量) + pendingGap(存量)
```

`spoolSize` 是投递缓存中的暂态存量，`pendingGap` 是已采集但因序列号缺口暂未放行的存量；
任一时刻每条 ACCEPTED 必落在六个桶之一。

## 7. HTTP 接口

| 方法 路径 | 作用 |
|---|---|
| `POST /api/ingest` | 上报一条原始读数（同时留存到 RawLog），返回 outcome/reason/event |
| `GET /api/devices` | 各设备最终读数、累计值、水位、连接状态 |
| `GET /api/metrics` | 全部分桶计数（含两个存量仪表） |
| `GET /api/reconcile` | 对账报告 + 恒等式校验 |
| `GET /api/decisions/{deviceId}` | 该设备完整判定链路（输入/依据/状态变化） |
| `GET /api/replay?version=v1` | 指定版本回放全部留存原始读数 |
| `POST /api/downstream/state?available=&delayMillis=` | 切换模拟下游可用性/发送延迟 |

## 8. 配置项（`application.properties`，均可命令行覆盖）

| 配置 | 默认 | 说明 |
|---|---|---|
| `gateway.spool.dir` | `./data/spool` | WAL、dead-letter、已确认键目录 |
| `gateway.spool.capacity` | `1000` | 投递缓存硬上限 |
| `gateway.rawlog.file` | `./data/raw/readings.log` | 原始读数 JSONL 留存 |
| `gateway.gap-buffer-capacity` | `64` | 每设备序列号缺口缓冲容量 |
| `gateway.max-clock-skew-millis` | `60000` | 允许的设备时间偏差 |
| `gateway.rate.permits-per-second` | `100.0` | 令牌桶速率 |
| `gateway.rate.burst` | `200` | 令牌桶容量 |
| `gateway.demo.enabled` | `false` | 是否运行端到端演示 |

## 9. 测试与本地验证清单

- `mvn test`：
  - `DeviceAggregateTest`：重复不双累计、迟到乱序拒绝、缺口缓冲与补齐按序应用、缓冲满拒绝；
  - `ReconnectIntegrationTest`：断线留存→重连回放→ACK 丢失重发→最终读数/累计一致；双设备隔离与单位换算；
  - `OrderingAndReplayTest`：乱序 seq0/seq2/seq1、缺口期间不提前投递、补齐连带投递；映射 v1/v2 回放可复现；
  - `DownstreamBackpressureTest`：BUFFERED 上限、RATE_LIMITED 与 DOWNSTREAM_DROPPED 原因可区分、
    恢复排空不丢、重复投递判 DUPLICATE_DELIVERY；
  - `PersistenceTest`：FileSpool 重启恢复 FIFO、已确认键重启后重复投递仍可辨认；
  - `IngestionValidationTest`：四类拒绝原因（坏质量/时间越界/未知点位/缺口超限）明确且计数守恒；
  - `ConcurrentDevicesTest`：3 台设备 × 200 条并发上报，各自水位连续、累计精确、全部恰好投递一次；
- 测试日志通过 SLF4J 与 `DecisionLog.dump` 打印**输入、判定依据与关键状态变化**（水位、pending、累计值）。
- 手工验证：见第 1 节演示命令与第 7 节 HTTP 接口。
