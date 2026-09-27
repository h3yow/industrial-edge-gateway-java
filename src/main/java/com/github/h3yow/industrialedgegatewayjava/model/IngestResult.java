package com.github.h3yow.industrialedgegatewayjava.model;

/**
 * 采集判定结果。
 *
 * @param outcome 分类
 * @param reason  当 outcome != ACCEPTED 时的具体原因，否则为 null
 * @param event   归一化结果，仅 ACCEPTED 时非 null
 */
public record IngestResult(IngestOutcome outcome, AnomalyReason reason, TelemetryEvent event) {

    public static IngestResult accepted(TelemetryEvent event) {
        return new IngestResult(IngestOutcome.ACCEPTED, null, event);
    }

    public static IngestResult duplicate() {
        return new IngestResult(IngestOutcome.DUPLICATE, null, null);
    }

    public static IngestResult outOfOrder() {
        return new IngestResult(IngestOutcome.OUT_OF_ORDER, null, null);
    }

    public static IngestResult rejected(AnomalyReason reason) {
        return new IngestResult(IngestOutcome.REJECTED, reason, null);
    }
}
