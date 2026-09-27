package com.github.h3yow.industrialedgegatewayjava.model;

/**
 * 下游投递的终态分类。六类互斥，保证采集/拒绝/丢弃数量可对账。
 */
public enum DeliveryStatus {
    /** 下游确认接收 */
    DELIVERED,
    /** 重复投递：幂等键已被投递服务确认过 */
    DUPLICATE_DELIVERY,
    /** 限流拒绝（限流令牌不足且缓存已满） */
    RATE_LIMITED,
    /** 下游不可用且缓存已满，被丢弃 */
    DOWNSTREAM_DROPPED,
    /** 已进入缓存（采集成功但尚未送达，重启后仍可恢复） */
    BUFFERED
}
