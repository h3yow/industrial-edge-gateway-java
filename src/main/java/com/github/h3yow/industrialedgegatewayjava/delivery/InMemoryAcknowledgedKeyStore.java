package com.github.h3yow.industrialedgegatewayjava.delivery;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存实现，用于测试。
 */
public class InMemoryAcknowledgedKeyStore implements AcknowledgedKeyStore {

    private final Set<String> keys = ConcurrentHashMap.newKeySet();

    @Override
    public void append(String dedupKey) {
        keys.add(dedupKey);
    }

    @Override
    public List<String> load() {
        return List.copyOf(keys);
    }
}
