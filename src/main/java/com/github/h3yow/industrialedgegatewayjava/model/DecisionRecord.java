package com.github.h3yow.industrialedgegatewayjava.model;

/**
 * 一条可解释的判定日志：输入、判定依据、关键状态变化。
 */
public record DecisionRecord(String stage,
                             String deviceId,
                             long sequence,
                             String decision,
                             String reason,
                             String detail) {
}
