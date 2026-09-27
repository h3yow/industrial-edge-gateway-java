package com.github.h3yow.industrialedgegatewayjava.downstream;

import com.github.h3yow.industrialedgegatewayjava.model.TelemetryPoint;

/**
 * 下游投递端点。实现方必须按 {@link TelemetryPoint#telemetryId()} 幂等：
 * 同一 id 重复投递只生效一次，且返回 duplicate=true，保证“重复投递不产生无法辨认的重复”。
 */
public interface DownstreamSink {

    SinkResult deliver(TelemetryPoint telemetry);

    /** 已幂等接受的不同 telemetryId 数量（对账用）。 */
    int deliveredDistinctCount();

    /** 下游是否可用（不可用时 dispatcher 走重试/死信策略）。 */
    boolean available();
}
