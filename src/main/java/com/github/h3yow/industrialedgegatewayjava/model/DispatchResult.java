package com.github.h3yow.industrialedgegatewayjava.model;

/**
 * 下游投递的终局/即时结果。
 *
 * @param outcome  投递结果类型
 * @param telemetryId 遥测标识
 * @param detail   可解释细节（拒绝原因、重试次数等）
 */
public record DispatchResult(
        DispatchOutcome outcome,
        String telemetryId,
        String detail
) {
}
