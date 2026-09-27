package com.github.h3yow.industrialedgegatewayjava.persistence;

import com.github.h3yow.industrialedgegatewayjava.model.TelemetryEvent;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * 有界 FIFO 内存缓存。offer 满时返回 false（拒绝依据：BUFFER_FULL）。
 * 线程安全，供投递 worker 与采集线程并发访问。
 */
public class InMemorySpool implements Spool {

    private final int capacity;
    private final ArrayDeque<TelemetryEvent> queue = new ArrayDeque<>();
    private final Object lock = new Object();

    public InMemorySpool(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.capacity = capacity;
    }

    @Override
    public boolean offer(TelemetryEvent event) {
        synchronized (lock) {
            if (queue.size() >= capacity) {
                return false;
            }
            queue.addLast(event);
            return true;
        }
    }

    @Override
    public void prepend(List<TelemetryEvent> events) {
        synchronized (lock) {
            java.util.List<TelemetryEvent> reversed = new java.util.ArrayList<>(events);
            java.util.Collections.reverse(reversed);
            for (TelemetryEvent event : reversed) {
                queue.addFirst(event);
            }
        }
    }

    @Override
    public List<TelemetryEvent> poll(int max) {
        synchronized (lock) {
            List<TelemetryEvent> out = new ArrayList<>(Math.min(max, queue.size()));
            while (out.size() < max && !queue.isEmpty()) {
                out.add(queue.pollFirst());
            }
            return out;
        }
    }

    @Override
    public int size() {
        synchronized (lock) {
            return queue.size();
        }
    }

    @Override
    public int capacity() {
        return capacity;
    }
}
