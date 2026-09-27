package com.github.h3yow.industrialedgegatewayjava.tests;

import com.github.h3yow.industrialedgegatewayjava.model.Quality;
import com.github.h3yow.industrialedgegatewayjava.model.RawReading;
import com.github.h3yow.industrialedgegatewayjava.sim.SimulatedDevice;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 设备侧行为：断线期间读数有界缓存，重连按序回放；溢出明确报 LOST_OFFLINE。
 */
class SimulatedDeviceReplayTest {

    private static final Logger log = LoggerFactory.getLogger(SimulatedDeviceReplayTest.class);

    @Test
    void offlineReadingsBufferedAndReplayedInOrderOnReconnect() {
        List<RawReading> wire = new ArrayList<>();
        SimulatedDevice device = new SimulatedDevice("device-A", "temp_c", "C", 3, wire::add);

        device.connect();
        assertNull(device.emit(1, Quality.GOOD, 0));
        log.info("[输入] 在线 seq=1 -> 立即发送");

        device.disconnect();
        log.info("[状态变化] 设备断线，后续读数进入离线缓存");
        for (int i = 0; i < 3; i++) {
            assertNull(device.emit(2 + i, Quality.GOOD, 0));
        }
        log.info("[状态] 离线缓存={} 上线发送={}", device.bufferedCount(), wire.size());

        // 缓存容量=3：再产生一条（seq=5）时溢出，丢弃的是最旧的 seq=2，必须明确可解释
        RawReading evicted = device.emit(0, Quality.BAD, 0);
        log.info("[输入] 离线第 4 条(seq=5) -> 被丢弃读数 seq={} 依据: 离线缓存容量上限=3",
                evicted == null ? null : evicted.sequence());
        assertNotNull(evicted, "溢出必须把被丢弃读数显式返回，不能悄悄丢失");
        assertEquals(2L, evicted.sequence());
        assertEquals(1, device.lostOfflineCount());

        device.connect();
        log.info("[状态变化] 重连，按序回放缓存，实际传输线上收到序列号={}",
                wire.stream().map(RawReading::sequence).toList());
        assertEquals(List.of(1L, 3L, 4L, 5L),
                wire.stream().map(RawReading::sequence).toList(),
                "溢出的 seq=2 丢失可解释，其余 seq=3,4,5 按序回放");
    }
}
