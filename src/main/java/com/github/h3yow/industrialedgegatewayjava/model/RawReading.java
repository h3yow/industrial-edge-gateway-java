package com.github.h3yow.industrialedgegatewayjava.model;

/**
 * 设备原始点位读数。
 *
 * @param deviceId      设备唯一标识
 * @param sequence      设备内单调递增序列号（跨断线连续），用于去重与乱序判定
 * @param pointId       原始点位标识
 * @param value         原始数值
 * @param unit          原始单位
 * @param quality       质量位
 * @param deviceTimeMs  设备时钟毫秒时间戳（主时间基准，由设备产生）
 */
public record RawReading(
        String deviceId,
        long sequence,
        String pointId,
        double value,
        String unit,
        Quality quality,
        long deviceTimeMs
) {
}
