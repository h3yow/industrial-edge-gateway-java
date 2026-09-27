package com.github.h3yow.industrialedgegatewayjava.ingest;

import com.github.h3yow.industrialedgegatewayjava.model.RawReading;

import java.util.ArrayList;
import java.util.List;

/**
 * 模拟设备连接层。
 *
 * <p>语义：
 * <ul>
 *   <li>连接期间 receive 直接返回待上报读数（{@link #receive} 返回值）；</li>
 *   <li>断线期间读数缓存在连接器侧（模拟设备/边缘侧的本地留存），不做幂等处理；</li>
 *   <li>重连后 {@link #drainReplayOnReconnect()} 把断线期间的读数以 duplicate 标记重发。
 *       是否真正重复由上层按序列号判定——设备重发的可能包含网关已收到的读数
 *       （如收到后 ACK 丢失），因此连接层不做“聪明”的去重。</li>
 * </ul>
 */
public class DeviceConnector {

    private final String deviceId;
    private volatile boolean connected;
    private final List<RawReading> offlineBuffer = new ArrayList<>();
    private final List<RawReading> liveReadings = new ArrayList<>();

    public DeviceConnector(String deviceId) {
        this.deviceId = deviceId;
    }

    public String deviceId() {
        return deviceId;
    }

    public void connect() {
        this.connected = true;
    }

    public void disconnect() {
        this.connected = false;
    }

    public boolean isConnected() {
        return connected;
    }

    /**
     * 接收一条来自设备的读数。
     *
     * @return 在线时返回该读数（供上层立即处理）；离线时返回 null（已留存待重连重发）
     */
    public RawReading receive(RawReading reading) {
        if (connected) {
            liveReadings.add(reading);
            return reading;
        }
        offlineBuffer.add(reading);
        return null;
    }

    /**
     * 重连后回放断线期间的读数，全部标记为传输层重发。回放后清空缓存。
     */
    public List<RawReading> drainReplayOnReconnect() {
        if (!connected) {
            throw new IllegalStateException("cannot replay while disconnected: " + deviceId);
        }
        List<RawReading> replay = new ArrayList<>(offlineBuffer.size());
        for (RawReading r : offlineBuffer) {
            replay.add(new RawReading(r.deviceId(), r.pointCode(), r.value(), r.unit(),
                    r.timestamp(), r.sequence(), r.quality(), true));
        }
        offlineBuffer.clear();
        return replay;
    }

    public int offlineBufferSize() {
        return offlineBuffer.size();
    }
}
