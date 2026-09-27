package com.github.h3yow.industrialedgegatewayjava.tests;

import com.github.h3yow.industrialedgegatewayjava.ingest.DeviceAggregate;
import com.github.h3yow.industrialedgegatewayjava.model.DeviceSnapshot;
import com.github.h3yow.industrialedgegatewayjava.model.RawReading;
import com.github.h3yow.industrialedgegatewayjava.model.TelemetryEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 序列号状态机：重复/乱序/缺口处理，最终读数与累计状态一致、不重复累计。
 */
class DeviceAggregateTest {

    private static final Logger log = LoggerFactory.getLogger(DeviceAggregateTest.class);

    private RawReading raw(long seq, double value) {
        return new RawReading("dev", "p", value, "u", 1000 + seq, seq, "GOOD", false);
    }

    private TelemetryEvent event(long seq, double value) {
        return new TelemetryEvent("dev", "m", value, "u", 1000 + seq, 2000, seq, "v1", "dev:" + seq);
    }

    @Test
    void duplicateSequenceIsNotAccumulatedAgain() {
        DeviceAggregate agg = new DeviceAggregate("dev");
        log.info("INPUT seq=0 value=10 (cumulative)");
        var r0 = agg.accept(raw(0, 10), event(0, 10), true);
        log.info("DECISION seq=0 => {} state: watermark={} totals={}",
                r0.decision(), agg.highWatermark(), agg.snapshot().totals());

        log.info("INPUT seq=0 value=10 DUPLICATE resend");
        var dup = agg.accept(raw(0, 10), event(0, 10), true);
        log.info("DECISION seq=0(resend) => {} state: watermark={} totals={}",
                dup.decision(), agg.highWatermark(), agg.snapshot().totals());

        assertEquals(DeviceAggregate.Decision.APPLIED, r0.decision());
        assertEquals(DeviceAggregate.Decision.DUPLICATE, dup.decision());
        DeviceSnapshot snap = agg.snapshot();
        assertEquals(10.0, snap.totals().get("m"), 1e-9, "累计值不能因重发翻倍");
        assertEquals(0, snap.highWatermark());
    }

    @Test
    void outOfOrderLateSequenceIsRejectedAndNeverAccumulated() {
        // seen 留存仅 4：旧序号被 LRU 淘汰后重放 -> 不是 DUPLICATE，而是 OUT_OF_ORDER，
        // 两种情况都不会重复累计。
        DeviceAggregate agg = new DeviceAggregate("dev", 64, 4);
        // 连续建立到 seq7，seq0..3 已被淘汰出 seen
        for (long s = 0; s <= 7; s++) {
            agg.accept(raw(s, s), event(s, s), false);
        }
        assertEquals(7, agg.highWatermark());

        // seq0 迟到重放：seen 已遗忘，但 seq < firstMissing(8)
        var late = agg.accept(raw(0, 0), event(0, 0), false);
        log.info("DECISION late seq=0 (evicted from seen) after watermark=7 => {} totals={}",
                late.decision(), agg.snapshot().totals());
        assertEquals(DeviceAggregate.Decision.OUT_OF_ORDER, late.decision());

        // 即使迟到读数是累计型，也绝不累计
        var lateCumulative = agg.accept(raw(1, 1), event(1, 1), true);
        assertEquals(DeviceAggregate.Decision.OUT_OF_ORDER, lateCumulative.decision());
        assertTrue(agg.snapshot().totals().getOrDefault("m", 0.0) == 0.0);
    }

    @Test
    void gapBufferedThenFilledInOrderAccumulatesExactlyOnce() {
        DeviceAggregate agg = new DeviceAggregate("dev");

        var a0 = agg.accept(raw(0, 10), event(0, 10), true);
        log.info("INPUT seq=0 => {} watermark={}", a0.decision(), agg.highWatermark());

        // 乱序：先到 seq2（缺口），再到 seq1
        var a2 = agg.accept(raw(2, 30), event(2, 30), true);
        log.info("INPUT seq=2 (gap) => {} pending={} watermark={}",
                a2.decision(), agg.pendingCount(), agg.highWatermark());
        assertEquals(DeviceAggregate.Decision.GAP_BUFFERED, a2.decision());
        assertEquals(10.0, agg.snapshot().totals().get("m"), 1e-9, "缺口期间不能提前累计");

        var a1 = agg.accept(raw(1, 20), event(1, 20), true);
        log.info("INPUT seq=1 (fills gap) => {} appliedEvents={} watermark={} totals={}",
                a1.decision(), a1.appliedEvents().size(), agg.highWatermark(), agg.snapshot().totals());

        assertEquals(DeviceAggregate.Decision.APPLIED, a1.decision());
        assertEquals(2, a1.appliedEvents().size(), "seq1 应用时应连带应用缓冲的 seq2");
        DeviceSnapshot snap = agg.snapshot();
        assertEquals(2, snap.highWatermark());
        assertEquals(60.0, snap.totals().get("m"), 1e-9, "10+20+30 每个序号恰好累计一次");
        assertEquals(30.0, snap.latestByMetric().get("m"), 1e-9, "最终读数为 seq2");
        assertEquals(0, agg.pendingCount());

        // seq2 再次重发 -> DUPLICATE，累计不变
        var dup2 = agg.accept(raw(2, 30), event(2, 30), true);
        assertEquals(DeviceAggregate.Decision.DUPLICATE, dup2.decision());
        assertEquals(60.0, agg.snapshot().totals().get("m"), 1e-9);
    }

    @Test
    void gapBufferFullIsRejected() {
        DeviceAggregate agg = new DeviceAggregate("dev", 2, 64);
        agg.accept(raw(0, 0), event(0, 0), false);
        // seq1 缺失，seq2 入缓冲（跨度 2 = 容量），seq3 跨度 3 > 容量
        assertEquals(DeviceAggregate.Decision.GAP_BUFFERED,
                agg.accept(raw(2, 2), event(2, 2), false).decision());
        var rejected = agg.accept(raw(3, 3), event(3, 3), false);
        assertEquals(DeviceAggregate.Decision.GAP_REJECTED, rejected.decision());
        log.info("DECISION seq=3 beyond gap-buffer capacity=2 => GAP_REJECTED (可解释的异常，不吞掉)");
    }
}
