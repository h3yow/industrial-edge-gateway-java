package com.github.h3yow.industrialedgegatewayjava.tests;

import com.github.h3yow.industrialedgegatewayjava.delivery.DeliveryService;
import com.github.h3yow.industrialedgegatewayjava.delivery.InMemoryAcknowledgedKeyStore;
import com.github.h3yow.industrialedgegatewayjava.delivery.SlowDownstreamSink;
import com.github.h3yow.industrialedgegatewayjava.delivery.TokenBucketRateLimiter;
import com.github.h3yow.industrialedgegatewayjava.ingest.IngestionService;
import com.github.h3yow.industrialedgegatewayjava.ingest.SimulatedDevice;
import com.github.h3yow.industrialedgegatewayjava.model.DeliveryStatus;
import com.github.h3yow.industrialedgegatewayjava.model.DeviceSnapshot;
import com.github.h3yow.industrialedgegatewayjava.model.IngestOutcome;
import com.github.h3yow.industrialedgegatewayjava.model.RawReading;
import com.github.h3yow.industrialedgegatewayjava.model.TelemetryEvent;
import com.github.h3yow.industrialedgegatewayjava.observability.DecisionLog;
import com.github.h3yow.industrialedgegatewayjava.observability.GatewayMetrics;
import com.github.h3yow.industrialedgegatewayjava.persistence.InMemorySpool;
import com.github.h3yow.industrialedgegatewayjava.replay.ReplayService;
import com.github.h3yow.industrialedgegatewayjava.transform.MappingRegistry;
import com.github.h3yow.industrialedgegatewayjava.transform.PointMapping;
import com.github.h3yow.industrialedgegatewayjava.transform.TelemetryTransformer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * 场景二：重复与乱序上报（端到端经 IngestionService + 投递层），
 * 以及映射规则版本化与历史回放可复现性。
 */
class OrderingAndReplayTest {

    private static final Logger log = LoggerFactory.getLogger(OrderingAndReplayTest.class);

    private record Harness(IngestionService ingestion, DeliveryService delivery,
                           GatewayMetrics metrics, DecisionLog decisions,
                           MappingRegistry registry, TelemetryTransformer transformer) {
    }

    private Harness harness() {
        MappingRegistry registry = new MappingRegistry();
        registry.register("v1", List.of(
                new PointMapping("device-A", "TEMP_c", "temperature_c", "C", 1, 0, false),
                new PointMapping("device-A", "TOTAL_flow_l", "total_volume_l", "L", 1, 0, true)
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
        return new Harness(ingestion, delivery, metrics, decisions, registry, transformer);
    }

    @Test
    void outOfOrderWithGapIsBufferedThenDeliveredInSequenceAndCountsReconcile() {
        Harness h = harness();
        SimulatedDevice device = new SimulatedDevice("device-A", 0);
        long t = System.currentTimeMillis();

        // 用固定序列号手工构造乱序：seq0 -> seq2(超前) -> seq1(补缺)
        RawReading r0 = new RawReading("device-A", "TOTAL_flow_l", 10, "L", t, 0, "GOOD", false);
        RawReading r2 = new RawReading("device-A", "TOTAL_flow_l", 30, "L", t + 2, 2, "GOOD", false);
        RawReading r1 = new RawReading("device-A", "TOTAL_flow_l", 20, "L", t + 1, 1, "GOOD", false);

        log.info("INPUT order: seq0, seq2(gap), seq1(fills gap)");
        assertEquals(IngestOutcome.ACCEPTED, h.ingestion().ingest(r0).outcome());
        assertEquals(IngestOutcome.ACCEPTED, h.ingestion().ingest(r2).outcome(),
                "seq2 超前但可缓冲，仍是采集成功（暂不投递）");
        assertEquals(false, h.delivery().isAcknowledged("device-A:2"),
                "seq2 在缺口补齐前不能投递");
        assertEquals(1, h.delivery().acknowledgedKeys(), "只有 seq0 已投递");

        assertEquals(IngestOutcome.ACCEPTED, h.ingestion().ingest(r1).outcome());

        DeviceSnapshot snap = h.ingestion().snapshots().get("device-A");
        log.info("FINAL snapshot after gap fill: {}", snap);
        assertEquals(2, snap.highWatermark());
        assertEquals(60.0, snap.totals().get("total_volume_l"), 1e-9, "10+20+30 恰好累计一次");
        assertEquals(30.0, snap.latestByMetric().get("total_volume_l"), 1e-9);
        for (long s = 0; s <= 2; s++) {
            assertEquals(true, h.delivery().isAcknowledged("device-A:" + s),
                    "seq" + s + " 补齐后必须已投递");
        }
        assertEquals(3, h.delivery().acknowledgedKeys());

        // 再次乱序重发 seq2 -> 重复
        assertEquals(IngestOutcome.DUPLICATE, h.ingestion().ingest(
                new RawReading("device-A", "TOTAL_flow_l", 30, "L", t + 2, 2, "GOOD", true)).outcome());
        assertEquals(60.0, h.ingestion().snapshots().get("device-A")
                .totals().get("total_volume_l"), 1e-9, "重复上报累计不变");

        log.info("RECONCILE:\n{}", h.metrics().reconcileReport(h.ingestion().totalPendingGap()));
        h.decisions().dump("device-A");
    }


    @Test
    void mappingVersioningMakesHistoricalReplayReproducible() {
        Harness h = harness();
        // v2：修改 device-A 温度映射——新规则假设原始值是开尔文偏移后的自定义刻度，
        // 这里用 factor=2.0 模拟规则调整。v1 保持不变且仍可回放。
        h.registry().register("v2", List.of(
                new PointMapping("device-A", "TEMP_c", "temperature_c", "C", 2.0, 0, false),
                new PointMapping("device-A", "TOTAL_flow_l", "total_volume_l", "L", 1, 0, true)
        ));
        assertEquals("v2", h.registry().currentVersion());

        List<RawReading> historical = List.of(
                new RawReading("device-A", "TEMP_c", 10, "C", 1000L, 0, "GOOD", false),
                new RawReading("device-A", "TEMP_c", 11, "C", 2000L, 1, "GOOD", false)
        );

        ReplayService replay = new ReplayService(h.transformer());
        List<TelemetryEvent> v1Result = replay.replay(historical, "v1");
        List<TelemetryEvent> v1Again = replay.replay(historical, "v1");
        List<TelemetryEvent> v2Result = replay.replay(historical, "v2");

        log.info("replay v1: {} | replay v2: {}", v1Result, v2Result);

        // 可复现：同版本两次回放完全一致
        assertEquals(v1Result, v1Again);
        assertEquals(10.0, v1Result.get(0).value(), 1e-9);
        assertEquals(20.0, v2Result.get(0).value(), 1e-9, "规则调整后 v2 结果不同");
        assertEquals("v1", v1Result.get(0).mappingVersion());
        assertEquals("v2", v2Result.get(0).mappingVersion());
        assertNotEquals(v1Result, v2Result, "版本之间结果可区分，且版本被固化在事件中");
    }
}
