package com.github.h3yow.industrialedgegatewayjava.ingest;

import com.github.h3yow.industrialedgegatewayjava.model.AnomalyReason;
import com.github.h3yow.industrialedgegatewayjava.model.DeviceSnapshot;
import com.github.h3yow.industrialedgegatewayjava.model.IngestResult;
import com.github.h3yow.industrialedgegatewayjava.model.RawReading;
import com.github.h3yow.industrialedgegatewayjava.model.TelemetryEvent;
import com.github.h3yow.industrialedgegatewayjava.observability.DecisionLog;
import com.github.h3yow.industrialedgegatewayjava.observability.GatewayMetrics;
import com.github.h3yow.industrialedgegatewayjava.transform.MappingRegistry;
import com.github.h3yow.industrialedgegatewayjava.transform.PointMapping;
import com.github.h3yow.industrialedgegatewayjava.transform.TelemetryTransformer;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * 采集入口，串行处理单设备读数。
 *
 * <p>校验顺序（先异常校验，后序号判定，确保异常不会被当成正常数据吞掉）：
 * <ol>
 *   <li>质量码非 GOOD => REJECTED/BAD_QUALITY；</li>
 *   <li>时间戳偏离网关时间超过上限 => REJECTED/TIMESTAMP_SKEW；</li>
 *   <li>当前映射版本查不到点位 => REJECTED/UNKNOWN_POINT；</li>
 *   <li>交给 {@link DeviceAggregate} 做重复/乱序/缺口判定。</li>
 * </ol>
 *
 * <p>缺口补齐后“连带应用”的事件同样是采集成功，逐条交给投递回调。
 */
public class IngestionService {

    private final MappingRegistry registry;
    private final TelemetryTransformer transformer;
    private final GatewayMetrics metrics;
    private final DecisionLog decisionLog;
    private final long maxClockSkewMillis;
    private final int gapBufferCapacity;

    /** 设备 -> 串行锁。 */
    private final Map<String, Object> locks = new ConcurrentHashMap<>();
    private final Map<String, DeviceAggregate> aggregates = new ConcurrentHashMap<>();

    /** 全部设备缺口暂存事件的总存量（在各设备锁内维护）。 */
    private final java.util.concurrent.atomic.AtomicLong pendingGapTotal =
            new java.util.concurrent.atomic.AtomicLong();

    /** 投递回调：把采集成功的事件交给投递层，可替换以便测试。 */
    private Function<TelemetryEvent, Void> deliverySink = e -> null;

    public IngestionService(MappingRegistry registry,
                            TelemetryTransformer transformer,
                            GatewayMetrics metrics,
                            DecisionLog decisionLog,
                            long maxClockSkewMillis,
                            int gapBufferCapacity) {
        this.registry = registry;
        this.transformer = transformer;
        this.metrics = metrics;
        this.decisionLog = decisionLog;
        this.maxClockSkewMillis = maxClockSkewMillis;
        this.gapBufferCapacity = gapBufferCapacity;
    }

    public void setDeliverySink(Function<TelemetryEvent, Void> deliverySink) {
        this.deliverySink = deliverySink;
    }

    public void updateConnection(String deviceId, boolean connected) {
        Object lock = locks.computeIfAbsent(deviceId, k -> new Object());
        synchronized (lock) {
            aggregate(deviceId).connected(connected);
            decisionLog.log("connection", deviceId, -1, connected ? "CONNECTED" : "DISCONNECTED",
                    null, "connection state changed");
        }
    }

    public IngestResult ingest(RawReading reading) {
        metrics.recordInput();
        String deviceId = reading.deviceId();
        Object lock = locks.computeIfAbsent(deviceId, k -> new Object());
        synchronized (lock) {
            DeviceAggregate agg = aggregate(deviceId);

            if (!"GOOD".equals(reading.quality())) {
                return reject(reading, AnomalyReason.BAD_QUALITY,
                        "quality=" + reading.quality());
            }
            long now = System.currentTimeMillis();
            if (Math.abs(now - reading.timestamp()) > maxClockSkewMillis) {
                return reject(reading, AnomalyReason.TIMESTAMP_SKEW,
                        "eventTime=%d gatewayTime=%d skew=%dms > %dms"
                                .formatted(reading.timestamp(), now, now - reading.timestamp(),
                                        maxClockSkewMillis));
            }
            String version = registry.currentVersion();
            Optional<PointMapping> rule = registry.find(version, deviceId, reading.pointCode());
            if (rule.isEmpty()) {
                return reject(reading, AnomalyReason.UNKNOWN_POINT,
                        "no mapping in version " + version + " for point " + reading.pointCode());
            }
            TelemetryEvent event = transformer.transform(reading, now, version).orElseThrow();
            DeviceAggregate.AcceptResult result =
                    agg.accept(reading, event, rule.get().cumulative());

            switch (result.decision()) {
                case DUPLICATE -> {
                    metrics.recordIngest(com.github.h3yow.industrialedgegatewayjava.model.IngestOutcome.DUPLICATE);
                    decisionLog.log("ingest", deviceId, reading.sequence(), "DUPLICATE", null,
                            "point=" + reading.pointCode() + " already processed; not accumulated again"
                                    + (reading.duplicate() ? " [transport resend]" : ""));
                    return IngestResult.duplicate();
                }
                case OUT_OF_ORDER -> {
                    metrics.recordIngest(com.github.h3yow.industrialedgegatewayjava.model.IngestOutcome.OUT_OF_ORDER);
                    decisionLog.log("ingest", deviceId, reading.sequence(), "OUT_OF_ORDER", null,
                            "seq below watermark=" + agg.highWatermark() + " and never seen; dropped, not accumulated");
                    return IngestResult.outOfOrder();
                }
                case GAP_REJECTED -> {
                    return reject(reading, AnomalyReason.GAP_SEQUENCE,
                            "gap buffer full (" + gapBufferCapacity + "), watermark=" + agg.highWatermark());
                }
                case GAP_BUFFERED -> {
                    metrics.recordIngest(com.github.h3yow.industrialedgegatewayjava.model.IngestOutcome.ACCEPTED);
                    metrics.setPendingGapGauge(pendingGapTotal.incrementAndGet());
                    decisionLog.log("ingest", deviceId, reading.sequence(), "ACCEPTED_BUFFERED", null,
                            "point=" + reading.pointCode() + " buffered; waiting for missing seq, watermark="
                                    + agg.highWatermark() + ", pending=" + agg.pendingCount());
                    // 已采集成功但暂不投递；补齐后随 APPLIED 一起投递。
                    return IngestResult.accepted(event);
                }
                case APPLIED -> {
                    metrics.recordIngest(com.github.h3yow.industrialedgegatewayjava.model.IngestOutcome.ACCEPTED);
                    // appliedEvents 中除第一条（当前读数）外，其余来自缺口暂存，需要扣减存量。
                    long released = result.appliedEvents().size() - 1L;
                    if (released > 0) {
                        metrics.setPendingGapGauge(pendingGapTotal.addAndGet(-released));
                    }
                    for (TelemetryEvent applied : result.appliedEvents()) {
                        deliverySink.apply(applied);
                        decisionLog.log("ingest", deviceId, applied.sequence(), "APPLIED", null,
                                "metric=" + applied.metricName() + " value=" + applied.value()
                                        + applied.unit() + " mapping=" + applied.mappingVersion()
                                        + " watermark=" + agg.highWatermark() + " totals=" + agg.snapshot().totals());
                    }
                    return IngestResult.accepted(event);
                }
                default -> throw new IllegalStateException("unknown decision " + result.decision());
            }
        }
    }

    private IngestResult reject(RawReading reading, AnomalyReason reason, String detail) {
        metrics.recordIngest(com.github.h3yow.industrialedgegatewayjava.model.IngestOutcome.REJECTED);
        decisionLog.log("ingest", reading.deviceId(), reading.sequence(), "REJECTED", reason.name(), detail);
        return IngestResult.rejected(reason);
    }

    private DeviceAggregate aggregate(String deviceId) {
        return aggregates.computeIfAbsent(deviceId,
                k -> new DeviceAggregate(k, gapBufferCapacity, 512));
    }

    public Map<String, DeviceSnapshot> snapshots() {
        Map<String, DeviceSnapshot> out = new java.util.LinkedHashMap<>();
        aggregates.forEach((id, agg) -> {
            Object lock = locks.get(id);
            if (lock == null) {
                return;
            }
            synchronized (lock) {
                out.put(id, agg.snapshot());
            }
        });
        return Map.copyOf(out);
    }

    public List<String> deviceIds() {
        return List.copyOf(aggregates.keySet());
    }

    /** 全部设备中“已采集成功但因序列号缺口暂存、尚未放行投递”的事件总数。 */
    public long totalPendingGap() {
        return pendingGapTotal.get();
    }
}
