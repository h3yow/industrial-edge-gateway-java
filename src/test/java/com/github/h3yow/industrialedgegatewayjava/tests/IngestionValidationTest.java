package com.github.h3yow.industrialedgegatewayjava.tests;

import com.github.h3yow.industrialedgegatewayjava.delivery.DeliveryService;
import com.github.h3yow.industrialedgegatewayjava.delivery.InMemoryAcknowledgedKeyStore;
import com.github.h3yow.industrialedgegatewayjava.delivery.SlowDownstreamSink;
import com.github.h3yow.industrialedgegatewayjava.delivery.TokenBucketRateLimiter;
import com.github.h3yow.industrialedgegatewayjava.ingest.IngestionService;
import com.github.h3yow.industrialedgegatewayjava.model.AnomalyReason;
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

/**
 * 采集层异常校验：坏质量、时间戳越界、未知点位、缺口超限，
 * 每条异常都有明确原因，且输入侧计数恒等。
 */
class IngestionValidationTest {

    private static final Logger log = LoggerFactory.getLogger(IngestionValidationTest.class);

    private RawReading r(long seq, String point, double v, String unit, long ts, String q) {
        return new RawReading("d", point, v, unit, ts, seq, q, false);
    }

    private IngestionService service(GatewayMetrics metrics, DecisionLog decisions, int gapCapacity) {
        MappingRegistry registry = new MappingRegistry();
        registry.register("v1", List.of(new PointMapping("d", "p", "m", "u", 1, 0, false)));
        TelemetryTransformer t = new TelemetryTransformer(registry);
        DeliveryService delivery = new DeliveryService(new SlowDownstreamSink(), new InMemorySpool(100),
                new TokenBucketRateLimiter(1_000_000, 1_000_000), metrics, decisions,
                new InMemoryAcknowledgedKeyStore());
        IngestionService svc = new IngestionService(registry, t, metrics, decisions, 60_000, gapCapacity);
        svc.setDeliverySink(e -> {
            delivery.accept(e);
            return null;
        });
        return svc;
    }

    @Test
    void allRejectionReasonsAreDistinctAndCountsReconcile() {
        GatewayMetrics metrics = new GatewayMetrics();
        DecisionLog decisions = new DecisionLog();
        IngestionService svc = service(metrics, decisions, 2);
        long now = System.currentTimeMillis();

        // seq0 正常，建立水位 0
        assertEquals(IngestOutcome.ACCEPTED,
                svc.ingest(r(0, "p", 0, "u", now, "GOOD")).outcome());
        // seq1 缺失：seq2 跨度 2 = 容量，可缓冲；seq3 跨度 3 > 容量，GAP_SEQUENCE
        assertEquals(IngestOutcome.ACCEPTED,
                svc.ingest(r(2, "p", 2, "u", now, "GOOD")).outcome());
        IngestResult gap = svc.ingest(r(3, "p", 3, "u", now, "GOOD"));
        log.info("gap over capacity: {} / {}", gap.outcome(), gap.reason());
        assertEquals(IngestOutcome.REJECTED, gap.outcome());
        assertEquals(AnomalyReason.GAP_SEQUENCE, gap.reason());

        // 其余三类异常（序列号继续向后，不影响上面的判定）
        IngestResult bad = svc.ingest(r(4, "p", 1, "u", now, "BAD"));
        IngestResult skew = svc.ingest(r(5, "p", 1, "u", now - 999_999_999L, "GOOD"));
        IngestResult unknown = svc.ingest(r(6, "OTHER", 1, "u", now, "GOOD"));
        log.info("rejections: bad={}/{} skew={}/{} unknown={}/{}",
                bad.outcome(), bad.reason(), skew.outcome(), skew.reason(),
                unknown.outcome(), unknown.reason());
        assertEquals(IngestOutcome.REJECTED, bad.outcome());
        assertEquals(AnomalyReason.BAD_QUALITY, bad.reason());
        assertEquals(AnomalyReason.TIMESTAMP_SKEW, skew.reason());
        assertEquals(AnomalyReason.UNKNOWN_POINT, unknown.reason());

        String report = metrics.reconcileReport(svc.totalPendingGap());
        log.info("RECONCILE:\n{}", report);
        assertEquals(6, metrics.totalInput());
        assertEquals(4, metrics.ingest(IngestOutcome.REJECTED));
        assertEquals(2, metrics.ingest(IngestOutcome.ACCEPTED),
                "seq0 已应用 + seq2 缺口缓冲，都是采集成功");
        decisions.dump("d");
    }
}
