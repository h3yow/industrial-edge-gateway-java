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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景一：多台设备断线重连 + 重复上报。
 * 断言：重复不二次累计；重连后序列号水位一致可解释。
 */
class ReconnectDuplicateTest {

    private static final Logger log = LoggerFactory.getLogger(ReconnectDuplicateTest.class);

    private RawReading r(String device, long seq, double value) {
        return new RawReading(device, seq, "temp_c", value, "C", Quality.GOOD, 1_000L + seq);
    }

    @Test
    void duplicateReportsAfterReconnectAreRejectedAndNotDoubleCounted() {
        DeviceSession session = new DeviceSession("device-A", 0L, 10);

        // 首次连接，上报 seq=1,2,3
        assertTrue(session.connect(), "首次 connect 应返回 true");
        for (long seq = 1; seq <= 3; seq++) {
            SessionOutcome o = session.handle(r("device-A", seq, 20.0 + seq));
            log.info("[输入] device-A seq={} 判定={} 释放={} 水位={}",
                    seq, o.decision(), o.released().size(), session.lastContiguous());
            assertEquals(RejectReason.ACCEPTED, o.decision());
        }
        assertEquals(3, session.lastContiguous(), "断连前水位应为 3");

        // 断线
        session.disconnect();
        log.info("[状态变化] device-A 断线 DISCONNECTED");

        // 重连后设备重放（至少一次语义导致 seq=1,2,3 再次到达）
        boolean reconnect = session.connect();
        assertTrue(reconnect, "disconnect 后再 connect 必须识别为重连");
        log.info("[状态变化] device-A 重连 CONNECTED，开始重放 seq=1..3");
        for (long seq = 1; seq <= 3; seq++) {
            SessionOutcome o = session.handle(r("device-A", seq, 20.0 + seq));
            log.info("[输入] 重放 device-A seq={} 判定={} 依据: seq<={} 视为重复 水位={}",
                    seq, o.decision(), session.lastContiguous(), session.lastContiguous());
            assertEquals(RejectReason.DUPLICATE, o.decision(), "重放数据必须判为 DUPLICATE");
            assertTrue(o.released().isEmpty());
        }

        // 重连后新数据继续推进，水位单调
        SessionOutcome next = session.handle(r("device-A", 4, 24.0));
        log.info("[输入] device-A seq=4 判定={} 释放={} 水位={}",
                next.decision(), next.released().size(), session.lastContiguous());
        assertEquals(RejectReason.ACCEPTED, next.decision());
        assertEquals(4, session.lastContiguous(), "最终水位必须为 4，重复数据未二次推进");
    }

    @Test
    void twoDevicesMaintainIndependentSequenceState() {
        DeviceSession a = new DeviceSession("device-A", 0L, 10);
        DeviceSession b = new DeviceSession("device-B", 0L, 10);
        a.connect();
        b.connect();

        a.handle(r("device-A", 1, 1));
        a.handle(r("device-A", 2, 2));
        b.handle(r("device-B", 1, 10));
        log.info("[状态] 多设备并行: device-A 水位={}, device-B 水位={}", a.lastContiguous(), b.lastContiguous());

        assertEquals(2, a.lastContiguous());
        assertEquals(1, b.lastContiguous());

        SessionOutcome dup = b.handle(r("device-B", 1, 10));
        assertEquals(RejectReason.DUPLICATE, dup.decision());
        log.info("[判定] device-B 重复 seq=1 -> {}，累计水位仍={}", dup.decision(), b.lastContiguous());
    }
}
