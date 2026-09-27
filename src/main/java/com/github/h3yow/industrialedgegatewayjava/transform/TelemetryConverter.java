package com.github.h3yow.industrialedgegatewayjava.transform;

import com.github.h3yow.industrialedgegatewayjava.mapping.MappingRegistry;
import com.github.h3yow.industrialedgegatewayjava.mapping.MappingVersion;
import com.github.h3yow.industrialedgegatewayjava.mapping.PointMapper;
import com.github.h3yow.industrialedgegatewayjava.model.Quality;
import com.github.h3yow.industrialedgegatewayjava.model.RawReading;
import com.github.h3yow.industrialedgegatewayjava.model.TelemetryPoint;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 遥测转换器：在指定映射版本下，把原始读数确定性地转换为对外遥测点。
 * 纯函数（除了注入的注册表/映射器）：同一原始读数 + 同一版本 ⇒ 同一结果（含 telemetryId），
 * 因此重复投递可幂等去重，规则调整后的历史回放可复现。
 */
public class TelemetryConverter {

    private final MappingRegistry registry;
    private final PointMapper mapper;

    public TelemetryConverter(MappingRegistry registry, PointMapper mapper) {
        this.registry = registry;
        this.mapper = mapper;
    }

    /**
     * 用当前映射版本转换（在线流水线使用）。
     */
    public Optional<TelemetryPoint> convert(RawReading reading, long ingestTimeMs) {
        return convert(reading, ingestTimeMs, registry.currentVersionName());
    }

    /**
     * 用指定映射版本转换（历史回放使用）。
     */
    public Optional<TelemetryPoint> convert(RawReading reading, long ingestTimeMs, String versionName) {
        MappingVersion version = registry.version(versionName);
        Optional<PointMapper.Resolved> resolved = mapper.resolve(reading, version);
        if (resolved.isEmpty()) {
            return Optional.empty();
        }
        PointMapper.Resolved r = resolved.get();

        Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put("rawPoint", reading.pointId());
        attributes.put("rawUnit", reading.unit());
        attributes.put("rawValue", Double.toString(reading.value()));
        attributes.put("sequence", Long.toString(reading.sequence()));
        if (reading.quality() != Quality.GOOD) {
            // 异常读数不进入累计器，但仍作为带标记的遥测对外可见，不会被悄悄吞掉
            attributes.put("abnormal", "true");
            attributes.put("qualityReason", reading.quality().name());
        }

        TelemetryPoint t = new TelemetryPoint(
                telemetryId(versionName, reading),
                reading.deviceId(),
                r.metricName(),
                r.value(),
                r.unit(),
                reading.quality(),
                reading.deviceTimeMs(),
                ingestTimeMs,
                versionName,
                Map.copyOf(attributes));
        return Optional.of(t);
    }

    /**
     * telemetryId 绑定映射版本/设备/序列号/点位：
     * 同一条数据（即使被重复投递）在同一映射版本下 id 恒定，下游可幂等。
     */
    public static String telemetryId(String versionName, RawReading reading) {
        String raw = versionName + "|" + reading.deviceId() + "|" + reading.sequence() + "|" + reading.pointId();
        String enc = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
        return "tel-" + enc;
    }
}
