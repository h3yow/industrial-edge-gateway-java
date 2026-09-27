package com.github.h3yow.industrialedgegatewayjava.model;

/**
 * 一条读数在网关上的终局判定，用于解释“为什么被接受/拒绝/丢弃”。
 */
public enum RejectReason {
    /** 首次通过连续性校验，正常进入处理流水线 */
    ACCEPTED,
    /** 设备序列号重复（重发/断线重连后的重复上报） */
    DUPLICATE,
    /** 到达时序列号落后于当前空洞窗口且无法再填补（乱序迟到，永久丢弃） */
    STALE,
    /** 质量位非 GOOD：不参与累计，但作为异常遥测对外可见 */
    BAD_QUALITY,
    /** 设备断连期间产生但未能随重连回放送达（溢出模拟缓存） */
    LOST_OFFLINE,
    /** 下游限流：未进入投递队列 */
    RATE_LIMITED,
    /** 下游缓存已满：被拒绝 */
    BUFFER_FULL,
    /** 下游不可用并重试耗尽：被丢弃（保留日志/计数，可辨认） */
    DEAD_LETTER
}
