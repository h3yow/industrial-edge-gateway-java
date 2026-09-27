package com.github.h3yow.industrialedgegatewayjava.mapping;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 映射版本注册表：保存全部历史版本与当前生效版本。
 * 注册表只读、线程安全；规则调整通过新增版本完成，不修改旧版本。
 */
public class MappingRegistry {

    private final Map<String, MappingVersion> versions = new LinkedHashMap<>();
    private final String currentVersionName;

    public MappingRegistry(String currentVersionName, MappingVersion... allVersions) {
        for (MappingVersion v : allVersions) {
            versions.put(v.name(), v);
        }
        if (!versions.containsKey(currentVersionName)) {
            throw new IllegalArgumentException("当前版本不存在: " + currentVersionName);
        }
        this.currentVersionName = currentVersionName;
    }

    public MappingVersion current() {
        return Objects.requireNonNull(versions.get(currentVersionName));
    }

    public MappingVersion version(String name) {
        MappingVersion v = versions.get(name);
        if (v == null) {
            throw new IllegalArgumentException("未知映射版本: " + name);
        }
        return v;
    }

    public String currentVersionName() {
        return currentVersionName;
    }

    public Set<String> versionNames() {
        return Set.copyOf(versions.keySet());
    }
}
