package com.github.h3yow.industrialedgegatewayjava.model;

import java.util.Map;

/**
 * 一台设备在网关侧的最终读数与累计状态快照。
 *
 * @param deviceId       设备标识
 * @param connected      当前连接状态
 * @param highWatermark  已确认的最大序列号
 * @param latestByMetric 每个指标最新一条归一化值
 * @param totals         按设备配置的累计指标（每个序列号只累计一次）
 */
public record DeviceSnapshot(String deviceId,
                             boolean connected,
                             long highWatermark,
                             Map<String, Double> latestByMetric,
                             Map<String, Double> totals) {
}
