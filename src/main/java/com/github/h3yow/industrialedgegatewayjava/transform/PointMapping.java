package com.github.h3yow.industrialedgegatewayjava.transform;

/**
 * 一条点位映射规则：原始点位 -> 统一指标。
 *
 * @param deviceId   适用设备
 * @param pointCode  原始点位编码
 * @param metricName 统一指标名
 * @param targetUnit 标准单位
 * @param factor     换算系数（标准值 = 原始值 * factor + offset）
 * @param offset     换算偏移
 * @param cumulative 是否为累计型指标
 */
public record PointMapping(String deviceId,
                           String pointCode,
                           String metricName,
                           String targetUnit,
                           double factor,
                           double offset,
                           boolean cumulative) {
}
