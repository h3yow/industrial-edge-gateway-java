package com.github.h3yow.industrialedgegatewayjava.model;

import java.util.List;

/**
 * 会话层对一次上报的判定结果（只负责去重/乱序/连续性，不做转换与投递）。
 *
 * @param decision       终局判定
 * @param incoming       本次上报读数
 * @param released       本次判定后从乱序缓存中按序释放的读数（可能含 incoming 本身）
 * @param anomalies      本次检测到的序列号空洞
 */
public record SessionOutcome(
        RejectReason decision,
        RawReading incoming,
        List<RawReading> released,
        List<GapAnomaly> anomalies
) {
}
