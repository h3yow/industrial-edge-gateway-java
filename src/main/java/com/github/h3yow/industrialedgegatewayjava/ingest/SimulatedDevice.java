package com.github.h3yow.industrialedgegatewayjava.ingest;

import com.github.h3yow.industrialedgegatewayjava.model.RawReading;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 本地模拟设备：按设备内单调递增序列号产生原始读数。
 *
 * <p>不发起真实连接，只负责读数构造；连接行为由 {@link DeviceConnector} 模拟。
 */
public class SimulatedDevice {

    private final String deviceId;
    private final AtomicLong sequence;

    public SimulatedDevice(String deviceId, long startSequence) {
        this.deviceId = deviceId;
        this.sequence = new AtomicLong(startSequence);
    }

    public SimulatedDevice(String deviceId) {
        this(deviceId, 0);
    }

    /**
     * 产生一条读数，时间戳默认取当前时间、质量 GOOD、非重发。
     */
    public RawReading reading(String pointCode, double value, String unit) {
        return reading(pointCode, value, unit, System.currentTimeMillis(), "GOOD", false);
    }

    public RawReading reading(String pointCode, double value, String unit, long timestamp, String quality) {
        return reading(pointCode, value, unit, timestamp, quality, false);
    }

    public RawReading reading(String pointCode, double value, String unit, long timestamp,
                              String quality, boolean duplicate) {
        return new RawReading(deviceId, pointCode, value, unit, timestamp,
                sequence.getAndIncrement(), quality, duplicate);
    }

    /** 用指定序列号重发一条读数（序列号不前进），用于模拟断线重连后的重传。 */
    public RawReading resend(RawReading original) {
        return new RawReading(original.deviceId(), original.pointCode(), original.value(),
                original.unit(), original.timestamp(), original.sequence(), original.quality(), true);
    }

    /** 下一个将分配的序列号（不断分配）。 */
    public long peekSequence() {
        return sequence.get();
    }
}
