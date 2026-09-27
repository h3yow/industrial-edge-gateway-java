package com.github.h3yow.industrialedgegatewayjava.transform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 映射规则注册表。
 *
 * <p>规则按版本不可变地保存：已发布的版本永远不会被修改，映射规则调整通过
 * 发布新版本（{@link #register}）完成。归一化时锁定一个版本，结果中带版本号；
 * 历史数据回放时指定当时的版本即可得到可复现的结论。
 *
 * <p>当前版本由注册顺序决定：最后注册的版本成为 currentVersion()。
 */
public class MappingRegistry {

    private record RuleKey(String deviceId, String pointCode) {
    }

    private final Map<String, Map<RuleKey, PointMapping>> versions = new ConcurrentHashMap<>();
    private volatile String currentVersion;

    public MappingRegistry() {
    }

    /**
     * 发布一个不可变版本。
     *
     * @throws IllegalArgumentException 版本重复
     */
    public synchronized void register(String version, List<PointMapping> rules) {
        if (versions.containsKey(version)) {
            throw new IllegalArgumentException("mapping version already exists: " + version);
        }
        Map<RuleKey, PointMapping> table = new HashMap<>();
        for (PointMapping rule : rules) {
            table.put(new RuleKey(rule.deviceId(), rule.pointCode()), rule);
        }
        versions.put(version, Map.copyOf(table));
        currentVersion = version;
    }

    public String currentVersion() {
        return currentVersion;
    }

    public List<String> versions() {
        return new ArrayList<>(versions.keySet());
    }

    public Optional<PointMapping> find(String version, String deviceId, String pointCode) {
        Map<RuleKey, PointMapping> table = versions.get(version);
        if (table == null) {
            throw new IllegalArgumentException("unknown mapping version: " + version);
        }
        return Optional.ofNullable(table.get(new RuleKey(deviceId, pointCode)));
    }
}
