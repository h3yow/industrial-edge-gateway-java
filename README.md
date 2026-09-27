# 工控边缘网关数据流程（本地可运行演示）

一套**完全本地化**、无外部服务依赖的边缘网关数据流水线：

```
模拟设备 ──上报/重连回放──▶ 会话层(去重/乱序/空洞) ──▶ 质量判定
      ──▶ 协议转换(点位映射/单位换算/版本化) ──▶ 累计状态(可重启恢复)
      ──▶ 背压投递(限流/有界缓存/重试/WAL/死信) ──▶ 本地内存下游(幂等)
```

技术栈：Java 21（可在 JDK 21+ 上编译运行）、Spring Boot 3.4、仅使用 spring-boot-starter-web/test
与 Jackson（随 Spring Boot 自带）。不连接任何外部 broker / 云服务，不需要真实凭据。

---

## 1. 快速开始

### 运行单元测试（含三类必测场景）

```bash
mvn test
```

测试日志会打印 **输入、判定依据、关键状态变化**（前缀 `[输入] / [判定] / [状态] / [状态变化] / [对账]`）。

### 启动本地服务

```bash
mvn package -DskipTests
# 方式一：纯手动模式（默认，不产生设备流量）
java -jar target/industrial-edge-gateway-java-1.0-SNAPSHOT.jar --server.port=18080

# 方式二：带 3 台模拟设备自动周期上报
java -jar target/industrial-edge-gateway-java-1.0-SNAPSHOT.jar \
     --server.port=18080 --gateway.simulator-enabled=true --gateway.simulator-interval-ms=300
```

数据文件默认写入 `target/gateway-data/`：

| 文件 | 内容 |
|---|---|
| `device-state.tsv` | 每设备序列号水位 + 每指标累计器（原子快照） |
| `downstream.wal` | 下游投递预写日志（`P`=PENDING，`C`=COMMIT，`X`=回滚） |
| `dead-letters.jsonl` | 重试耗尽的死信遥测（含原因、尝试次数） |

---

## 2. 设备接入与转换规则

### 2.1 多设备接入与断线重连

* 每台设备用 `deviceId` 维护独立会话；设备产生的 **`sequence` 序列号跨断线单调递增**。
* 设备端（`SimulatedDevice`）断线期间把读数写入**有界离线缓存**；重连后**按序回放**（至少一次语义）。
  缓存满时丢弃**最旧**读数，丢弃会返回被丢弃读数并累加 `lostOffline`（语义 `LOST_OFFLINE`），不静默丢失。
* 网关端会话状态机（`DeviceSession`）对每条读数给出确定判定：

| 情况 | 判定 | 含义 |
|---|---|---|
| `seq == 水位+1` | `ACCEPTED` | 连续，立即释放处理 |
| `seq <= 水位` | `DUPLICATE` | 重复上报（重发/重连回放），**不转换、不累计、不投递** |
| 同序号已在乱序窗口 | `DUPLICATE` | 重复上报 |
| `seq > 水位+1` | `ACCEPTED`（入 pending）+ **空洞告警** | 乱序早到，等待空洞填补后按序释放 |
| pending 窗口已满且非下一序号 | `STALE` | 永久迟到/越序，明确拒绝 |

* 序列号**空洞**（gap）在“向前跳跃”时产生一条 `GapAnomaly`（区间、设备、检测时间），
  通过 ingest 响应与计数（`gaps`）显式暴露；空洞未填补期间数据不会被提前释放。
* 会话初始水位从持久化状态加载，**服务重启后重放旧数据仍然是 DUPLICATE**，不会重复累计。

### 2.2 协议转换（点位映射 / 单位换算 / 时间基准）

* 规则是不可变的 `MappingRule`：`标准值 = 原始值 * scale + offset`，点位匹配支持精确名与尾部 `*` 通配。
* 规则集合按**版本**发布（`MappingVersion`），注册表（`MappingRegistry`）保留全部历史版本：
  * `mapping-v1`：`temp_c → temperature(°C)` 直传；`temp_f → °C`；`pressure_kpa → pressure(Pa)`（×1000）。
  * `mapping-v2`（当前生效）：温度基准改为 **K**（`temp_c + 273.15`），压力规则不变。
* 调整规则 = 新增版本；旧版本永不修改，因此**历史数据按旧版本回放结果可复现**。
* 遥测 `telemetryId = Base64Url(版本|设备|序列号|点位)`：同一条数据在同一版本下 id 恒定，
  下游据此幂等；不同版本下 id 不同，规则变更可辨认。
* 时间基准：`eventTimeMs` 恒取**设备时钟**（`RawReading.deviceTimeMs`），
  网关处理时间单独存于 `ingestTimeMs`，两者不混用，回放时保持原值。

---

## 3. 异常数据处理语义

| 异常 | 处理 |
|---|---|
| 重复上报 | `DUPLICATE`：直接拒绝，不进任何累计/投递；计数可查 |
| 乱序 | 有界 pending 按序释放；窗口外 `STALE` 拒绝 |
| 序列号空洞 | `GapAnomaly` 显式记录（区间 + 检测时间），计数 `gaps` |
| 坏质量读数 `BAD/UNCERTAIN` | 判定 `BAD_QUALITY`：转成带 `attributes.abnormal=true / qualityReason` 的遥测**照常投递**，但**不写入累计器**（count/sum/min/max 不被污染），连续性水位仍推进 |
| 未映射点位 | 计入 `unmapped`，原始读数仍归档；无遥测、无投递 |
| 设备离线缓存溢出 | 设备侧返回被丢弃读数 + `LOST_OFFLINE` 计数 |
| 下游失败重试耗尽 | `DEAD_LETTER`：写入 `dead-letters.jsonl` 与内存死信列表，WAL 不打 COMMIT，可人工辨认/重放 |

原则：**任何异常都有名字、有计数、有记录，不会被当成正常数据悄悄吞掉。**

---

## 4. 下游背压与重复投递行为

`BackpressureDispatcher` 采用“先限流、再有界缓存、后重试/死信”的三级策略，每一级都有**明确依据与上限**：

1. **限流 `RATE_LIMITED`** — 令牌桶（`gateway.rate-permits-per-second` /
   `rate-burst-capacity`）。拿不到令牌立即拒绝，**消息不进队列**，返回细节说明“可用令牌=0”。
2. **缓存拒绝 `BUFFER_FULL`** — 两道独立上限，细节会标明命中哪一道：
   * 有界投递队列（`gateway.buffer-capacity`）；
   * WAL 未确认记录上限（`gateway.wal-max-pending`）。
3. **缓存投递 `QUEUED`** — 入队前先写 WAL `PENDING`，单线程 worker 顺序投递。
4. **重试与死信** — 下游不可用/异常时按 `delivery-max-attempts` 重试（固定退避
   `delivery-retry-backoff-ms`）；仍失败记 `DEAD_LETTER`。
5. **重启恢复** — 启动时扫描 WAL，所有“有 P 无 C/X”记录重新投递；下游按 `telemetryId` 幂等。
6. **重复投递** — 本地 sink 用 `telemetryId` 去重：重复数据只落一次，并返回 `duplicate=true`；
   即使发生“数据已落库但 ACK 丢失”，重试也只会命中幂等分支，按一次成功对账，
   既不悄悄丢失，也不会产生无法辨认的重复。

### 计数对账（`GET /api/metrics`）

* 入口：`received = accepted + badQuality + duplicate + stale (+ lostOffline)`
* 投递（对映射成功的读数）：
  `accepted 的映射成功数 = delivered + rateLimited + bufferFull + deadLetter`
  （`QUEUED` 只是中间态，最终归入 delivered/deadLetter）
* 另提供 `unmapped`、`gaps`、`reconnects`、`queueDepth`、
  `downstreamDistinctDelivered`、`downstreamDuplicateDeliveries`、`deadLetters`。

累计器（`GET /api/state`）只统计 GOOD 读数：`count / sum / min / max / average / last*`。

---

## 5. HTTP 接口（本地验证）

| 方法与路径 | 说明 |
|---|---|
| `POST /api/devices/{id}/connect` / `disconnect` | 建立/断开设备会话（重连有 `reconnected` 标记） |
| `POST /api/ingest` | 上报一条原始读数（JSON：deviceId/sequence/pointId/value/unit/quality?/deviceTimeMs?），返回判定、转换结果、投递结论、空洞 |
| `GET  /api/state` | 序列号水位与各设备各指标累计器 |
| `GET  /api/metrics` | 全量对账计数与队列深度 |
| `GET  /api/telemetry` | 归档原始读数按**当前版本**的转换结果 |
| `GET  /api/replay?version=mapping-v1` | 按指定版本确定性回放历史原始读数 |
| `GET  /api/mappings` | 当前映射版本与全部可用版本 |
| `GET  /api/dead-letters` | 死信详情（原因/尝试次数/原始遥测） |
| `POST /api/downstream/available?available=false` | 模拟下游不可用 |
| `POST /api/downstream/delay?ms=400` | 模拟下游处理变慢（制造阻塞） |
| `POST /api/simulator/{id}/disconnect` / `reconnect` | 对模拟设备制造断线/重连（离线缓存回放） |

### curl 演示片段

```bash
B=http://localhost:18080/api
curl -X POST $B/devices/device-A/connect
# 正常（25°C 在 v2 下换算为 298.15K）
curl -X POST $B/ingest -H 'Content-Type: application/json' \
  -d '{"deviceId":"device-A","sequence":1,"pointId":"temp_c","value":25,"unit":"C"}'
# 重复 -> DUPLICATE
curl -X POST $B/ingest -H 'Content-Type: application/json' \
  -d '{"deviceId":"device-A","sequence":1,"pointId":"temp_c","value":25,"unit":"C"}'
# 坏质量 -> BAD_QUALITY，遥测带 abnormal 标记，不累计
curl -X POST $B/ingest -H 'Content-Type: application/json' \
  -d '{"deviceId":"device-A","sequence":2,"pointId":"temp_c","value":999,"unit":"C","quality":"BAD"}'
# 乱序：先 5 后 3、4（seq5 先入 pending 并报空洞，3、4 到达后按序释放 3,4,5）
curl -X POST $B/ingest -H 'Content-Type: application/json' \
  -d '{"deviceId":"device-A","sequence":5,"pointId":"temp_f","value":68,"unit":"F"}'
curl -X POST $B/ingest -H 'Content-Type: application/json' \
  -d '{"deviceId":"device-A","sequence":3,"pointId":"temp_c","value":26,"unit":"C"}'
# 版本化回放
curl "$B/replay?version=mapping-v1"   # °C
curl "$B/replay?version=mapping-v2"   # K
```

制造下游阻塞并观察限流/缓存满：

```bash
curl -X POST "$B/downstream/delay?ms=500"
# 用极小缓冲启动：--gateway.buffer-capacity=3 --gateway.rate-burst-capacity=2
# 连续上报即可分别看到 QUEUED / RATE_LIMITED / BUFFER_FULL
curl "$B/metrics"
```

---

## 6. 测试覆盖

`src/test/java/.../tests/` 下 14 个用例，包含要求的三类场景与重启/版本语义：

| 测试类 | 覆盖内容 |
|---|---|
| `ReconnectDuplicateTest` | 断线重连、重放重复被拒、多设备水位独立、不重复推进 |
| `OutOfOrderTest` | 乱序缓存按序释放、空洞区间精确报告、窗口满 STALE |
| `BackpressureDispatcherTest` | RATE_LIMITED 与 BUFFER_FULL 可区分且有上限、不可用重试→DEAD_LETTER、ACK 丢失重投幂等 |
| `SimulatedDeviceReplayTest` | 设备离线有界缓存、溢出 LOST_OFFLINE、重连按序回放 |
| `IngestionPipelineTest` | 端到端计数对账、坏质量不累计、状态快照重启恢复、WAL 未确认记录重启重放恰好一次 |
| `MappingVersionReplayTest` | 单位换算、v1/v2 版本切换、telemetryId 绑定版本、回放确定可复现、时间基准、未映射点位 |

---

## 7. 主要配置项（`gateway.*`）

| 配置 | 默认值 | 说明 |
|---|---|---|
| `data-dir` | `target/gateway-data` | 状态/WAL/死信目录 |
| `simulator-enabled` | `false` | 是否启动模拟设备 |
| `simulator-devices` / `simulator-interval-ms` | 3 / 500 | 模拟设备数与周期 |
| `session-max-pending` | 100 | 每设备乱序窗口上限 |
| `rate-permits-second` / `rate-burst-capacity` | 20 / 10 | 限流速率与桶容量 |
| `buffer-capacity` | 20 | 下游有界队列容量 |
| `wal-max-pending` | 200 | WAL 未确认记录上限 |
| `delivery-max-attempts` / `delivery-retry-backoff-ms` | 3 / 50 | 重试次数与退避 |
| `flush-state-per-read` | true | 每条释放读数后刷状态快照 |
