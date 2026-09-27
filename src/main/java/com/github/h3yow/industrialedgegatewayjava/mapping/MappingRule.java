package com.github.h3yow.industrialedgegatewayjava.mapping;

import java.util.Optional;

/**
 * 单条点位映射规则（不可变）。
 * 匹配支持精确点位名或以 {@code *} 结尾的前缀通配。
 * 换算公式：standardValue = rawValue * scale + offset。
 *
 * @param pointIdPattern 原始点位匹配模式
 * @param metricName     统一后的对外指标名
 * @param scale          线性换算系数
 * @param offset         线性换算偏移
 * @param targetUnit     标准单位
 */
public record MappingRule(
        String pointIdPattern,
        String metricName,
        double scale,
        double offset,
        String targetUnit
) {
    public boolean matches(String pointId) {
        if (pointIdPattern.endsWith("*")) {
            String prefix = pointIdPattern.substring(0, pointIdPattern.length() - 1);
            return pointId.startsWith(prefix);
        }
        return pointIdPattern.equals(pointId);
    }

    public double convert(double rawValue) {
        return rawValue * scale + offset;
    }
}
