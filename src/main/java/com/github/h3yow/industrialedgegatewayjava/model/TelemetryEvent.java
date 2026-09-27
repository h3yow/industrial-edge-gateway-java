package com.github.h3yow.industrialedgegatewayjava.model;

/**
 * 归一化后的对外遥测结果。
 *
 * @param deviceId       设备标识
 * @param metricName     统一指标名
 * @param value          换算后的数值
 * @param unit           标准单位
 * @param eventTime      事件时间（设备时间，epoch 毫秒 UTC）
 * @param ingestTime     网关接收时间（epoch 毫秒 UTC）
 * @param sequence       设备侧序列号
 * @param mappingVersion 生成该结果所用映射规则版本
 * @param dedupKey       幂等键：deviceId + ":" + sequence
 */
public record TelemetryEvent(String deviceId,
                             String metricName,
                             double value,
                             String unit,
                             long eventTime,
                             long ingestTime,
                             long sequence,
                             String mappingVersion,
                             String dedupKey) {
}
