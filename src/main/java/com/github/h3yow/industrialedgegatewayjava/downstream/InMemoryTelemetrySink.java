package com.github.h3yow.industrialedgegatewayjava.downstream;

import com.github.h3yow.industrialedgegatewayjava.model.TelemetryPoint;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地内存下游（不接外部服务）。
 * <ul>
 *   <li>按 telemetryId 幂等：重复投递不重复计数，并明确返回 duplicate；</li>
 *   <li>可切换可用/不可用、可注入“处理慢”延迟，用于演示下游阻塞；</li>
 *   <li>可注入“ACK 丢失”：数据实际已落库但返回失败，用于验证重试不产生重复。</li>
 * </ul>
 */
public class InMemoryTelemetrySink implements DownstreamSink {

    private final Map<String, TelemetryPoint> delivered = new ConcurrentHashMap<>();
    private final Set<String> ackLossIds = ConcurrentHashMap.newKeySet();
    private final AtomicInteger duplicateDeliveries = new AtomicInteger();
    private volatile boolean availableFlag = true;
    private volatile long processDelayMs = 0L;

    @Override
    public SinkResult deliver(TelemetryPoint telemetry) {
        if (processDelayMs > 0) {
            try {
                Thread.sleep(processDelayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return SinkResult.fail("interrupted");
            }
        }
        if (!availableFlag) {
            return SinkResult.fail("downstream unavailable");
        }
        TelemetryPoint previous = delivered.putIfAbsent(telemetry.telemetryId(), telemetry);
        if (previous != null) {
            duplicateDeliveries.incrementAndGet();
            return SinkResult.duplicate(telemetry.telemetryId());
        }
        if (ackLossIds.remove(telemetry.telemetryId())) {
            return SinkResult.fail("simulated ack lost (data persisted)");
        }
        return SinkResult.ok();
    }

    @Override
    public int deliveredDistinctCount() {
        return delivered.size();
    }

    @Override
    public boolean available() {
        return availableFlag;
    }

    public TelemetryPoint get(String telemetryId) {
        return delivered.get(telemetryId);
    }

    public int duplicateDeliveryCount() {
        return duplicateDeliveries.get();
    }

    public void setAvailable(boolean available) {
        this.availableFlag = available;
    }

    public void setProcessDelayMs(long delayMs) {
        this.processDelayMs = delayMs;
    }

    /** 下一条同 id 的消息模拟“ACK 丢失但数据已落库”。 */
    public void simulateAckLoss(String telemetryId) {
        ackLossIds.add(telemetryId);
    }

    public void clear() {
        delivered.clear();
        ackLossIds.clear();
        duplicateDeliveries.set(0);
    }
}
