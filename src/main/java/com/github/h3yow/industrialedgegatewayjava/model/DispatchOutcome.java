package com.github.h3yow.industrialedgegatewayjava.model;

/**
 * 投递结果分类。DELIVERED 为成功；QUEUED 为已进入有界缓存等待异步投递；
 * 其余均为拒绝/丢弃，计入对账计数。
 */
public enum DispatchOutcome {
    DELIVERED,
    QUEUED,
    RATE_LIMITED,
    BUFFER_FULL,
    DEAD_LETTER
}
