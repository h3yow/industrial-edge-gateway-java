package com.github.h3yow.industrialedgegatewayjava.model;

import java.util.List;

/**
 * 一条读数进入网关后的完整处理结论（采集层返回给调用方）。
 *
 * @param reading   原始读数
 * @param decision  终局判定（ACCEPTED/DUPLICATE/STALE/BAD_QUALITY/...）
 * @param telemetry 转换结果；未映射或非有效读数时为 null
 * @param dispatch  投递结果；未进入投递时为 null
 * @param anomalies 处理中检测到的空洞异常
 * @param duplicates 本次连带按序释放的其它读数（便于调用方观察批量状态变化）
 */
public record IngestResult(
        RawReading reading,
        RejectReason decision,
        TelemetryPoint telemetry,
        DispatchResult dispatch,
        List<GapAnomaly> anomalies,
        List<RawReading> duplicates
) {
}
