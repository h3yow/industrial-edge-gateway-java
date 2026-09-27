package com.github.h3yow.industrialedgegatewayjava.tests;

import com.github.h3yow.industrialedgegatewayjava.mapping.MappingRegistry;
import com.github.h3yow.industrialedgegatewayjava.mapping.MappingVersions;
import com.github.h3yow.industrialedgegatewayjava.mapping.PointMapper;
import com.github.h3yow.industrialedgegatewayjava.model.Quality;
import com.github.h3yow.industrialedgegatewayjava.model.RawReading;
import com.github.h3yow.industrialedgegatewayjava.model.TelemetryPoint;
import com.github.h3yow.industrialedgegatewayjava.transform.TelemetryConverter;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景四（映射版本化）：单位换算 + 版本切换 + 历史回放可复现。
 */
class MappingVersionReplayTest {

    private static final Logger log = LoggerFactory.getLogger(MappingVersionReplayTest.class);

    private final MappingRegistry registry =
            new MappingRegistry(MappingVersions.V2, MappingVersions.v1(), MappingVersions.v2());
    private final TelemetryConverter converter = new TelemetryConverter(registry, new PointMapper());

    @Test
    void unitConversionStableAndVersionedReplayIsReproducible() {
        RawReading celsius = new RawReading("device-A", 1, "temp_c", 10.0, "C",
                Quality.GOOD, 1000L);
        RawReading fahrenheit = new RawReading("device-B", 1, "temp_f", 68.0, "F",
                Quality.GOOD, 1000L);

        TelemetryPoint v1C = converter.convert(celsius, 2000L, MappingVersions.V1).orElseThrow();
        TelemetryPoint v1F = converter.convert(fahrenheit, 2000L, MappingVersions.V1).orElseThrow();
        log.info("[输入] v1: 10°C -> {} {}; 68°F -> {} {}",
                v1C.value(), v1C.unit(), v1F.value(), v1F.unit());
        assertEquals(10.0, v1C.value(), 1e-9);
        assertEquals("C", v1C.unit());
        assertEquals(20.0, v1F.value(), 1e-9, "68°F 必须换算为 20°C");

        TelemetryPoint v2C = converter.convert(celsius, 2000L, MappingVersions.V2).orElseThrow();
        log.info("[输入] v2: 10°C -> {} {}（规则调整：温度基准改为开尔文）", v2C.value(), v2C.unit());
        assertEquals(283.15, v2C.value(), 1e-9);
        assertEquals("K", v2C.unit());

        // telemetryId 绑定版本：同一原始数据在不同版本下是可辨认的两条结果
        assertNotEquals(v1C.telemetryId(), v2C.telemetryId());
        assertTrue(v1C.telemetryId().startsWith("tel-"));
        log.info("[判定] telemetryId 绑定版本: v1={} v2={}", v1C.telemetryId(), v2C.telemetryId());

        // 历史回放：同输入 + 同版本 ⇒ 同结果（确定性，可复现）
        TelemetryPoint replay = converter.convert(celsius, 2000L, MappingVersions.V1).orElseThrow();
        assertEquals(v1C.telemetryId(), replay.telemetryId());
        assertEquals(v1C.value(), replay.value(), 1e-9);
        assertEquals(MappingVersions.V1, replay.mappingVersion());
        log.info("[回放] 使用 v1 再次转换历史读数 -> {} {} id={}（与首次完全一致）",
                replay.value(), replay.unit(), replay.telemetryId());

        // 时间基准稳定：事件时间取设备时钟，不受网关 ingest 时间影响
        assertEquals(1000L, replay.eventTimeMs());
        assertEquals(2000L, replay.ingestTimeMs());
    }

    @Test
    void unknownPointIsUnmappedAndVisible() {
        RawReading weird = new RawReading("device-A", 2, "vibration_x", 3.0, "mm/s",
                Quality.GOOD, 1000L);
        Optional<TelemetryPoint> none = converter.convert(weird, 2000L, MappingVersions.V1);
        log.info("[输入] 未映射点位 vibration_x -> {}（将计入 unmapped，原始读数仍归档）",
                none.isPresent() ? "MAPPED" : "UNMAPPED");
        assertTrue(none.isEmpty());
    }
}
