package com.github.h3yow.industrialedgegatewayjava.transform;

import com.github.h3yow.industrialedgegatewayjava.model.RawReading;
import com.github.h3yow.industrialedgegatewayjava.model.TelemetryEvent;

import java.util.Optional;

/**
 * 协议转换。
 *
 * <ul>
 *   <li>点位映射：原始 (deviceId, pointCode) 在指定版本中查统一指标；</li>
 *   <li>单位换算：standard = raw * factor + offset；</li>
 *   <li>时间基准：事件时间保留设备时间戳（epoch 毫秒，UTC），另记录网关接收时间；</li>
 *   <li>版本固化：结果携带 mappingVersion，回放指定相同版本可复现。</li>
 * </ul>
 */
public class TelemetryTransformer {

    private final MappingRegistry registry;

    public TelemetryTransformer(MappingRegistry registry) {
        this.registry = registry;
    }

    /**
     * @return 归一化结果；该版本下无此点位时返回 empty（由上层判为 REJECTED/UNKNOWN_POINT）
     */
    public Optional<TelemetryEvent> transform(RawReading reading, long ingestTime, String mappingVersion) {
        Optional<PointMapping> mapped = registry.find(mappingVersion, reading.deviceId(), reading.pointCode());
        if (mapped.isEmpty()) {
            return Optional.empty();
        }
        PointMapping rule = mapped.get();
        double standardValue = reading.value() * rule.factor() + rule.offset();
        String dedupKey = reading.deviceId() + ":" + reading.sequence();
        return Optional.of(new TelemetryEvent(
                reading.deviceId(),
                rule.metricName(),
                standardValue,
                rule.targetUnit(),
                reading.timestamp(),
                ingestTime,
                reading.sequence(),
                mappingVersion,
                dedupKey));
    }
}
