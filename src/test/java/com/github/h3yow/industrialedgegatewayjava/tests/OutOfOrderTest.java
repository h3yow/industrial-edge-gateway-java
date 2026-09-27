package com.github.h3yow.industrialedgegatewayjava.tests;

import com.github.h3yow.industrialedgegatewayjava.model.Quality;
import com.github.h3yow.industrialedgegatewayjava.model.RawReading;
import com.github.h3yow.industrialedgegatewayjava.model.RejectReason;
import com.github.h3yow.industrialedgegatewayjava.model.SessionOutcome;
import com.github.h3yow.industrialedgegatewayjava.session.DeviceSession;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景二：乱序到达 + 空洞 + 永久迟到。
 * 断言：乱序缓存按序释放、空洞被显式报告、迟到/重复各有可解释判定。
 */
class OutOfOrderTest {

    private static final Logger log = LoggerFactory.getLogger(OutOfOrderTest.class);

    private RawReading r(long seq) {
        return new RawReading("device-A", seq, "temp_c", seq, "C", Quality.GOOD, 1000L + seq);
    }

    @Test
    void gapIsReportedAndOutOfOrderReleasesInSequence() {
        DeviceSession s = new DeviceSession("device-A", 0L, 10);
        s.connect();

        // 正常收到 1
        SessionOutcome o1 = s.handle(r(1));
        assertEquals(RejectReason.ACCEPTED, o1.decision());
        assertEquals(List.of(1L), o1.released().stream().map(RawReading::sequence).toList());
        log.info("[输入] seq=1 判定=ACCEPTED 释放=[1] 水位={}", s.lastContiguous());

        // seq=3 早到：应检出空洞 [2,2]，但不释放（等 2）
        SessionOutcome o3 = s.handle(r(3));
        log.info("[输入] seq=3 判定={} 释放={} 空洞={} pending={}",
                o3.decision(),
                o3.released().stream().map(RawReading::sequence).toList(),
                o3.anomalies().stream().map(g -> "[" + g.firstMissing() + "," + g.lastMissing() + "]").toList(),
                s.pendingSequences());
        assertEquals(RejectReason.ACCEPTED, o3.decision());
        assertTrue(o3.released().isEmpty(), "seq=2 未到，不能释放 seq=3");
        assertEquals(1, o3.anomalies().size(), "必须显式报告空洞");
        assertEquals(2, o3.anomalies().get(0).firstMissing());
        assertEquals(2, o3.anomalies().get(0).lastMissing());
        assertEquals(1, s.lastContiguous(), "水位不能越过空洞");

        // seq=3 重复
        SessionOutcome dup = s.handle(r(3));
        assertEquals(RejectReason.DUPLICATE, dup.decision());
        log.info("[输入] seq=3 再次到达 判定=DUPLICATE 依据: 已在 pending 中");

        // seq=2 到达：连带按序释放 2,3，水位到 3，无空洞
        SessionOutcome o2 = s.handle(r(2));
        log.info("[输入] seq=2 判定={} 连带释放={} 空洞={} 水位={}",
                o2.decision(),
                o2.released().stream().map(RawReading::sequence).toList(),
                o2.anomalies().size(), s.lastContiguous());
        assertEquals(RejectReason.ACCEPTED, o2.decision());
        assertEquals(List.of(2L, 3L), o2.released().stream().map(RawReading::sequence).toList());
        assertTrue(o2.anomalies().isEmpty(), "空洞填补时不应重复报异常");
        assertEquals(3, s.lastContiguous());
        assertEquals(0, s.pendingCount());
    }

    @Test
    void staleReadingBeyondFullPendingWindowIsRejectedNotSilentlyDropped() {
        DeviceSession s = new DeviceSession("device-A", 0L, 2);
        s.connect();
        s.handle(r(1));

        // 制造乱序把 pending 窗口填满：seq=3,4
        SessionOutcome o3 = s.handle(r(3));
        SessionOutcome o4 = s.handle(r(4));
        assertEquals(RejectReason.ACCEPTED, o3.decision());
        assertEquals(RejectReason.ACCEPTED, o4.decision());
        log.info("[状态] pending 已满 pending={} 水位={}", s.pendingSequences(), s.lastContiguous());

        // seq=10 远跳且窗口已满 -> STALE，明确拒绝而不是静默丢弃
        SessionOutcome stale = s.handle(r(10));
        log.info("[输入] seq=10 判定={} 依据: pending 窗口已满且非下一个连续序号", stale.decision());
        assertEquals(RejectReason.STALE, stale.decision());
        assertFalse(stale.anomalies().isEmpty(), "跳号仍应报告空洞");

        // 空洞填补后状态仍可解释
        SessionOutcome fill2 = s.handle(r(2));
        log.info("[输入] seq=2 判定={} 连带释放={} 水位={} pending={}",
                fill2.decision(),
                fill2.released().stream().map(RawReading::sequence).toList(),
                s.lastContiguous(), s.pendingSequences());
        assertEquals(4, s.lastContiguous());
    }
}
