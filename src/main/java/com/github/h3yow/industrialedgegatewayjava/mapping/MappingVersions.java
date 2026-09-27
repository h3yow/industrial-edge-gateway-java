package com.github.h3yow.industrialedgegatewayjava.mapping;

import java.util.List;

/**
 * 内置映射版本集合，演示规则版本化。
 * <ul>
 *   <li>mapping-v1：摄氏温（temp_c）直传为 °C；华氏温（temp_f）换算为 °C；千帕（pressure_kpa）换算为 Pa。</li>
 *   <li>mapping-v2：温度基准改为开尔文 K（同一原始数据回放会得到可复现但不同的结果）；压力规则不变。</li>
 * </ul>
 */
public final class MappingVersions {

    public static final String V1 = "mapping-v1";
    public static final String V2 = "mapping-v2";

    private MappingVersions() {
    }

    public static MappingVersion v1() {
        return new MappingVersion(V1, List.of(
                new MappingRule("temp_c", "temperature", 1.0, 0.0, "C"),
                new MappingRule("temp_f", "temperature", 5.0 / 9.0, -32.0 * 5.0 / 9.0, "C"),
                new MappingRule("pressure_kpa", "pressure", 1000.0, 0.0, "Pa")
        ));
    }

    public static MappingVersion v2() {
        return new MappingVersion(V2, List.of(
                new MappingRule("temp_c", "temperature", 1.0, 273.15, "K"),
                new MappingRule("temp_f", "temperature", 5.0 / 9.0, 273.15 - 32.0 * 5.0 / 9.0, "K"),
                new MappingRule("pressure_kpa", "pressure", 1000.0, 0.0, "Pa")
        ));
    }
}
