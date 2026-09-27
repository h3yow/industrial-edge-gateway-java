package com.github.h3yow.industrialedgegatewayjava.mapping;

import com.github.h3yow.industrialedgegatewayjava.model.RawReading;

import java.util.Optional;

/**
 * 点位转换器：在给定映射版本下把原始读数换算为标准值。纯函数，无副作用，保证回放可复现。
 */
public class PointMapper {

    /**
     * 转换结果。
     */
    public record Resolved(String metricName, double value, String unit, MappingRule matchedRule) {
    }

    public Optional<Resolved> resolve(RawReading reading, MappingVersion version) {
        for (MappingRule rule : version.rules()) {
            if (rule.matches(reading.pointId())) {
                return Optional.of(new Resolved(
                        rule.metricName(),
                        rule.convert(reading.value()),
                        rule.targetUnit(),
                        rule));
            }
        }
        return Optional.empty();
    }
}
