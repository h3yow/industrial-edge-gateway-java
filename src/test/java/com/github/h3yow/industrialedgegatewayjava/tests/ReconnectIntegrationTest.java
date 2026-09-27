package com.github.h3yow.industrialedgegatewayjava.tests;

import com.github.h3yow.industrialedgegatewayjava.delivery.DeliveryService;
import com.github.h3yow.industrialedgegatewayjava.delivery.InMemoryAcknowledgedKeyStore;
import com.github.h3yow.industrialedgegatewayjava.delivery.SlowDownstreamSink;
import com.github.h3yow.industrialedgegatewayjava.delivery.TokenBucketRateLimiter;
import com.github.h3yow.industrialedgegatewayjava.ingest.DeviceConnector;
import com.github.h3yow.industrialedgegatewayjava.ingest.IngestionService;
import com.github.h3yow.industrialedgegatewayjava.ingest.SimulatedDevice;
import com.github.h3yow.industrialedgegatewayjava.model.DeliveryStatus;
import com.github.h3yow.industrialedgegatewayjava.model.DeviceSnapshot;
import com.github.h3yow.industrialedgegatewayjava.model.IngestOutcome;
import com.github.h3yow.industrialedgegatewayjava.model.IngestResult;
import com.github.h3yow.industrialedgegatewayjava.model.RawReading;
import com.github.h3yow.industrialedgegatewayjava.observability.DecisionLog;
import com.github.h3yow.industrialedgegatewayjava.observability.GatewayMetrics;
import com.github.h3yow.industrialedgegatewayjava.persistence.InMemorySpool;
import com.github.h3yow.industrialedgegatewayjava.transform.MappingRegistry;
import com.github.h3yow.industrialedgegatewayjava.transform.PointMapping;
import com.github.h3yow.industrialedgegatewayjava.transform.TelemetryTransformer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景一：多设备同时接入、断线重连、断线期间数据回放、重复重发不重复累计。
 */
class ReconnectIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ReconnectIntegrationTest.class);

    private record Harness(IngestionService ingestion, DeliveryService delivery,
                           SlowDownstreamSink sink, GatewayMetrics metrics,
                           DecisionLog decisions) {
    }

    private Harness harness() {
        MappingRegistry registry = new MappingRegistry();
        registry.register("v1", List.of(
                new PointMapping("device-A", "TEMP_c", "temperature_c", "C", 1, 0, false),
                new PointMapping("device-A", "TOTAL_flow_l", "total_volume_l", "L", 1, 0, true),
                new PointMapping("device-B", "temp_f", "temperature_c", "C",
                        5.0d / 9.0d, -32.0d * 5.0d / 9.0d, false)
        ));
        TelemetryTransformer transformer = new TelemetryTransformer(registry);
        GatewayMetrics metrics = new GatewayMetrics();
        DecisionLog decisions = new DecisionLog();
        SlowDownstreamSink sink = new SlowDownstreamSink();
        DeliveryService delivery = new DeliveryService(sink, new InMemorySpool(100),
                new TokenBucketRateLimiter(1_000_000, 1_000_000), metrics, decisions,
                new InMemoryAcknowledgedKeyStore());
        IngestionService ingestion = new IngestionService(registry, transformer, metrics,
                decisions, 60_000, 64);
        ingestion.setDeliverySink(e -> {
            delivery.accept(e);
            return null;
        });
        return new Harness(ingestion, delivery, sink, metrics, decisions);
    }

    @Test
    void disconnectReplayAndDuplicateResendYieldsConsistentTotals() {
        Harness h = harness();
        SimulatedDevice device = new SimulatedDevice("device-A", 0);
        DeviceConnector connector = new DeviceConnector("device-A");

        // 接入后上报 seq0(温度) seq1(累计100)
        connector.connect();
        h.ingestion().updateConnection("device-A", true);
        h.ingestion().ingest(connector.receive(
                device.reading("TEMP_c", 20.0, "C", System.currentTimeMillis(), "GOOD")));
        h.ingestion().ingest(connector.receive(
                device.reading("TOTAL_flow_l", 100.0, "L", System.currentTimeMillis(), "GOOD")));

        // 断线：期间读数由连接器留存
        connector.disconnect();
        h.ingestion().updateConnection("device-A", false);
        RawReading offlineTemp = device.reading("TEMP_c", 21.0, "C",
                System.currentTimeMillis(), "GOOD");
        RawReading offlineTotal = device.reading("TOTAL_flow_l", 50.0, "L",
                System.currentTimeMillis(), "GOOD");
        connector.receive(offlineTemp);
        connector.receive(offlineTotal);
        log.info("INPUT while offline: seq={} (temp), seq={} (total +50) buffered in connector",
                offlineTemp.sequence(), offlineTotal.sequence());

        // 重连回放
        connector.connect();
        h.ingestion().updateConnection("device-A", true);
        for (RawReading r : connector.drainReplayOnReconnect()) {
            IngestResult result = h.ingestion().ingest(r);
            log.info("REPLAY seq={} => {}", r.sequence(), result.outcome());
        }

        // ACK 丢失场景：seq1(累计100) 被设备再次重发
        RawReading resend = device.resend(new RawReading("device-A", "TOTAL_flow_l", 100.0,
                "L", System.currentTimeMillis(), 1, "GOOD", false));
        IngestResult duplicate = h.ingestion().ingest(resend);
        log.info("INPUT resend seq=1 => {} (must not accumulate again)", duplicate.outcome());

        // 异常数据不能被当正常数据吞掉：坏质量
        IngestResult bad = h.ingestion().ingest(device.reading("TEMP_c", 99, "C",
                System.currentTimeMillis(), "BAD"));
        assertEquals(IngestOutcome.REJECTED, bad.outcome());
        assertEquals(com.github.h3yow.industrialedgegatewayjava.model.AnomalyReason.BAD_QUALITY,
                bad.reason());

        DeviceSnapshot snap = h.ingestion().snapshots().get("device-A");
        log.info("FINAL device snapshot: {}", snap);
        h.decisions().dump("device-A");

        assertEquals(150.0, snap.totals().get("total_volume_l"), 1e-9,
                "累计只应有 seq1(100)+seq3(50)=150，重发的 seq1 不能再累计");
        assertEquals(21.0, snap.latestByMetric().get("temperature_c"), 1e-9,
                "最终温度应为断线后回放的 21.0");
        assertEquals(3, snap.highWatermark(), "水位应推进到 seq3");
        assertEquals(IngestOutcome.DUPLICATE, duplicate.outcome());

        // 对账恒等式
        String report = h.metrics().reconcileReport(h.ingestion().totalPendingGap());
        log.info("RECONCILE:\n{}", report);
        assertEquals(1, h.metrics().ingest(IngestOutcome.DUPLICATE));
        assertEquals(1, h.metrics().ingest(IngestOutcome.REJECTED));
        // 所有正常事件均已投递，缓存与缺口为空
        assertEquals(0, h.delivery().spoolSize());
        assertEquals(4, h.metrics().delivery(DeliveryStatus.DELIVERED));
        assertEquals(0, h.sink().acknowledgedCount() - 4, "下游确认的幂等键恰好 4 个");
    }

    @Test
    void twoDevicesHaveIndependentSequenceSpacesAndUnits() {
        Harness h = harness();
        SimulatedDevice a = new SimulatedDevice("device-A", 0);
        SimulatedDevice b = new SimulatedDevice("device-B", 0);

        // 两台设备都发 seq0，互不影响；device-B 华氏度 68F 应归一为 20C
        h.ingestion().ingest(a.reading("TEMP_c", 25.0, "C", System.currentTimeMillis(), "GOOD"));
        IngestResult rb = h.ingestion().ingest(
                b.reading("temp_f", 68.0, "F", System.currentTimeMillis(), "GOOD"));

        assertEquals(IngestOutcome.ACCEPTED, rb.outcome());
        DeviceSnapshot snapA = h.ingestion().snapshots().get("device-A");
        DeviceSnapshot snapB = h.ingestion().snapshots().get("device-B");
        log.info("device-A snapshot {} | device-B snapshot {}", snapA, snapB);
        assertEquals(0, snapA.highWatermark());
        assertEquals(0, snapB.highWatermark(), "两台设备序列号空间独立");
        assertEquals(25.0, snapA.latestByMetric().get("temperature_c"), 1e-9);
        assertEquals(20.0, snapB.latestByMetric().get("temperature_c"), 1e-9,
                "68F 必须换算为 20C");

        // device-A 发一个未映射点位 -> 拒绝原因明确
        IngestResult unknown = h.ingestion().ingest(
                a.reading("NOT_MAPPED", 1, "X", System.currentTimeMillis(), "GOOD"));
        assertEquals(IngestOutcome.REJECTED, unknown.outcome());
        assertEquals(com.github.h3yow.industrialedgegatewayjava.model.AnomalyReason.UNKNOWN_POINT,
                unknown.reason());
    }
}
