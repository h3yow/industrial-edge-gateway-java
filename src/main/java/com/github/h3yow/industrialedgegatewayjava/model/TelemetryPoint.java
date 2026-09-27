package com.github.h3yow.industrialedgegatewayjava.model;

import java.util.Map;

/**
 * 转换后的对外遥测点。
 *
 * @param telemetryId   遥测唯一标识，由 映射版本/设备/序列号/点位 派生，保证重复投递可去重
 * @param deviceId      设备
 * @param metricName    统一后的指标名
 * @param value         换算后的数值
 * @param unit          标准单位
 * @param quality       质量位
 * @param eventTimeMs   事件时间（设备时钟，稳定时间基准）
 * @param ingestTimeMs  网关采集时间
 * @param mappingVersion 产生该结果所用映射规则版本
 * @param attributes    附加属性（如原始点位、原始单位、是否异常等）
 */
public record TelemetryPoint(
        String telemetryId,
        String deviceId,
        String metricName,
        double value,
        String unit,
        Quality quality,
        long eventTimeMs,
        long ingestTimeMs,
        String mappingVersion,
        Map<String, String> attributes
) {
}
