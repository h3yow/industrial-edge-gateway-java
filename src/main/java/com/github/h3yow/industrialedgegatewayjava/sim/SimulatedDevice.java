package com.github.h3yow.industrialedgegatewayjava.sim;

import com.github.h3yow.industrialedgegatewayjava.model.Quality;
import com.github.h3yow.industrialedgegatewayjava.model.RawReading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.Consumer;

/**
 * 本地模拟设备（不接任何真实协议/服务）。
 *
 * 模拟的传输行为：
 * <ul>
 *   <li>序列号跨断线单调递增（设备侧生成，不依赖连接状态）；</li>
 *   <li>断线期间读数进入有界缓存；缓存满后最旧数据被丢弃并明确返回 LOST_OFFLINE（不静默吞）；</li>
 *   <li>重连后先按序回放缓存读数，再发送新读数（至少一次语义）；</li>
 *   <li>可配置“重发份数”，制造重复上报；也可注入乱序发送与坏质量读数。</li>
 * </ul>
 */
public class SimulatedDevice {

    private static final Logger log = LoggerFactory.getLogger(SimulatedDevice.class);

    private final String deviceId;
    private final String pointId;
    private final String unit;
    private final int offlineBufferCapacity;
    private final Consumer<RawReading> transport;

    private long sequence = 0;
    private boolean connected = false;
    private final Deque<RawReading> offlineBuffer = new ArrayDeque<>();
    private long lostOffline = 0;

    public SimulatedDevice(String deviceId, String pointId, String unit,
                           int offlineBufferCapacity, Consumer<RawReading> transport) {
        this.deviceId = deviceId;
        this.pointId = pointId;
        this.unit = unit;
        this.offlineBufferCapacity = offlineBufferCapacity;
        this.transport = transport;
    }

    public void connect() {
        if (connected) {
            return;
        }
        connected = true;
        // 重连：先按序回放断连期间缓存（至少一次），调用方可据此验证去重与不重复累计
        List<RawReading> replay = new ArrayList<>(offlineBuffer);
        offlineBuffer.clear();
        for (RawReading r : replay) {
            transport.accept(r);
        }
    }

    public void disconnect() {
        connected = false;
    }

    public boolean isConnected() {
        return connected;
    }

    /**
     * 产生一条读数。在线直接上报，离线进入有界缓存。
     *
     * @param duplicateCopies 额外重复发送份数（0=不重复；&gt;0 模拟重发）
     * @return 因离线缓存溢出而被丢弃的读数（含其序列号，可解释）；null 表示无丢失
     */
    public RawReading emit(double value, Quality quality, int duplicateCopies) {
        sequence++;
        RawReading reading = new RawReading(deviceId, sequence, pointId, value, unit,
                quality, System.currentTimeMillis());
        if (!connected) {
            RawReading evicted = null;
            if (offlineBuffer.size() >= offlineBufferCapacity) {
                // 有界缓存满：丢弃最旧数据并计数（显式可追溯），新读数仍进入缓存
                evicted = offlineBuffer.pollFirst();
                lostOffline++;
                log.warn("[LOST_OFFLINE] 设备={} 离线缓存满(容量={})，丢弃最旧读数 seq={}，新读数 seq={} 入缓存",
                        deviceId, offlineBufferCapacity, evicted.sequence(), sequence);
            }
            offlineBuffer.addLast(reading);
            return evicted;
        }
        sendWithCopies(reading, duplicateCopies);
        return null;
    }

    /**
     * 以乱序方式直接发送一条原始读数（绕过本地缓存，测试网关乱序处理）。
     */
    public void emitRawOutOfOrder(long seq, double value, Quality quality, long deviceTimeMs) {
        transport.accept(new RawReading(deviceId, seq, pointId, value, unit, quality, deviceTimeMs));
    }

    private void sendWithCopies(RawReading reading, int duplicateCopies) {
        transport.accept(reading);
        for (int i = 0; i < duplicateCopies; i++) {
            transport.accept(reading);
        }
    }

    public long currentSequence() {
        return sequence;
    }

    public int bufferedCount() {
        return offlineBuffer.size();
    }

    public long lostOfflineCount() {
        return lostOffline;
    }

    public String deviceId() {
        return deviceId;
    }
}
