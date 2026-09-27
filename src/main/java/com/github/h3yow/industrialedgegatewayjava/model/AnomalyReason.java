package com.github.h3yow.industrialedgegatewayjava.model;

/**
 * 异常数据的具体原因，用于可解释的拒绝判定。
 */
public enum AnomalyReason {
    BAD_QUALITY,
    TIMESTAMP_SKEW,
    UNKNOWN_POINT,
    GAP_SEQUENCE,
    DOWNSTREAM_UNAVAILABLE
}
