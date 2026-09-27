package com.github.h3yow.industrialedgegatewayjava.persistence;

import com.github.h3yow.industrialedgegatewayjava.model.TelemetryEvent;

import java.util.List;

/**
 * 待投递事件的有界缓存（持久化 WAL，重启不丢）。
 */
public interface Spool {

    /** 入缓存（队尾）；容量不足时返回 false（调用方据此转为拒绝/丢弃）。 */
    boolean offer(TelemetryEvent event);

    /**
     * 把一批事件按原顺序放回队首（投递失败重试，保证不丢且保持顺序）。
     * 实现方必须保证在曾 poll 出同等数量后有足够空间。
     */
    void prepend(List<TelemetryEvent> events);

    /** 取出并移除最早的若干条。 */
    List<TelemetryEvent> poll(int max);

    int size();

    int capacity();
}
