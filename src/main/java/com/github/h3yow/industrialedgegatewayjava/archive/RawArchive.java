package com.github.h3yow.industrialedgegatewayjava.archive;

import com.github.h3yow.industrialedgegatewayjava.model.RawReading;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * 原始读数归档（有界、内存、线程安全），仅用于本地演示与映射版本回放：
 * 保存全部曾被会话层接受的原始读数，容量超出后丢弃最旧数据。
 */
public class RawArchive {

    private final int capacity;
    private final ConcurrentLinkedDeque<RawReading> readings = new ConcurrentLinkedDeque<>();

    public RawArchive(int capacity) {
        this.capacity = capacity;
    }

    public synchronized void append(RawReading reading) {
        readings.addLast(reading);
        while (readings.size() > capacity) {
            readings.pollFirst();
        }
    }

    /** 按设备、序列号稳定排序后返回快照（保证回放顺序可复现）。 */
    public synchronized List<RawReading> replay() {
        return readings.stream()
                .sorted(java.util.Comparator
                        .comparing(RawReading::deviceId)
                        .thenComparingLong(RawReading::sequence))
                .toList();
    }

    public synchronized int size() {
        return readings.size();
    }

    public synchronized void clear() {
        readings.clear();
    }
}
