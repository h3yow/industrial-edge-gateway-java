package com.github.h3yow.industrialedgegatewayjava.observability;

import com.github.h3yow.industrialedgegatewayjava.model.DeliveryStatus;
import com.github.h3yow.industrialedgegatewayjava.model.IngestOutcome;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 可对账计数器。
 *
 * <p>对账恒等式（以原始上报为输入）：
 * <pre>
 * 输入总数 = ACCEPTED + DUPLICATE + OUT_OF_ORDER + REJECTED
 * ACCEPTED = DELIVERED + DUPLICATE_DELIVERY + RATE_LIMITED + DOWNSTREAM_DROPPED
 *            + spoolSize(存量仪表) + pendingGap(存量仪表)
 * </pre>
 * 其中：
 * <ul>
 *   <li>四个 delivery.* 为<b>终态</b>累计计数（重启不重置的计数语义在本实现中按进程生命周期）；</li>
 *   <li>spoolSize 是仍在投递缓存中的存量（BUFFERED 暂态），由投递服务实时同步；</li>
 *   <li>pendingGap 是已采集成功、但因序列号缺口尚未放行投递的存量。</li>
 * </ul>
 * 任一时刻每条 ACCEPTED 必处于六个桶之一，因此“采集成功、被拒绝、被丢弃”的数量可以对齐。
 */
public class GatewayMetrics {

    private final Map<IngestOutcome, AtomicLong> ingestCounts = new ConcurrentHashMap<>();
    private final Map<DeliveryStatus, AtomicLong> deliveryCounts = new ConcurrentHashMap<>();
    private final AtomicLong totalInput = new AtomicLong();
    private final AtomicLong bufferedGauge = new AtomicLong();
    private final AtomicLong pendingGapGauge = new AtomicLong();

    public GatewayMetrics() {
        for (IngestOutcome o : IngestOutcome.values()) {
            ingestCounts.put(o, new AtomicLong());
        }
        for (DeliveryStatus s : DeliveryStatus.values()) {
            deliveryCounts.put(s, new AtomicLong(0));
        }
    }

    public void recordInput() {
        totalInput.incrementAndGet();
    }

    public void recordIngest(IngestOutcome outcome) {
        ingestCounts.get(outcome).incrementAndGet();
    }

    public void recordDelivery(DeliveryStatus status) {
        deliveryCounts.get(status).incrementAndGet();
    }

    /** BUFFERED 是暂态存量而非终态：由投递服务在缓存增减时同步为实际 spoolSize。 */
    public void setBufferedGauge(long value) {
        bufferedGauge.set(value);
    }

    /** 已采集但因序列号缺口尚未放行投递的存量。 */
    public void setPendingGapGauge(long value) {
        pendingGapGauge.set(value);
    }

    public long totalInput() {
        return totalInput.get();
    }

    public long ingest(IngestOutcome outcome) {
        return ingestCounts.get(outcome).get();
    }

    public long delivery(DeliveryStatus status) {
        return deliveryCounts.get(status).get();
    }

    public Map<String, Long> snapshot() {
        Map<String, Long> out = new LinkedHashMap<>();
        out.put("input", totalInput.get());
        for (IngestOutcome o : IngestOutcome.values()) {
            out.put("ingest." + o.name(), ingestCounts.get(o).get());
        }
        // 终态累计（BUFFERED 不作为累计计数，单独以存量仪表输出）
        for (DeliveryStatus s : DeliveryStatus.values()) {
            if (s == DeliveryStatus.BUFFERED) {
                continue;
            }
            out.put("delivery." + s.name(), deliveryCounts.get(s).get());
        }
        out.put("delivery.BUFFERED.gauge", bufferedGauge.get());
        out.put("ingest.PENDING_GAP.gauge", pendingGapGauge.get());
        return out;
    }

    /**
     * 生成对账报告并校验两侧恒等式。
     *
     * @param pendingGap 当前因缺口暂存、未放行投递的采集成功事件数
     * @throws IllegalStateException 任一恒等式不成立时抛出
     */
    public String reconcileReport(long pendingGap) {
        long input = totalInput.get();
        long classified = 0;
        for (IngestOutcome o : IngestOutcome.values()) {
            classified += ingestCounts.get(o).get();
        }
        if (input != classified) {
            throw new IllegalStateException(
                    "metrics mismatch: input=%d but classified=%d".formatted(input, classified));
        }

        long accepted = ingestCounts.get(IngestOutcome.ACCEPTED).get();
        long terminal = 0;
        for (DeliveryStatus s : DeliveryStatus.values()) {
            if (s != DeliveryStatus.BUFFERED) {
                terminal += deliveryCounts.get(s).get();
            }
        }
        long inFlight = bufferedGauge.get() + pendingGap;
        if (accepted != terminal + inFlight) {
            throw new IllegalStateException(
                    "delivery mismatch: accepted=%d but terminal=%d + inFlight(spool=%d+gap=%d)=%d"
                            .formatted(accepted, terminal, bufferedGauge.get(), pendingGap,
                                    terminal + inFlight));
        }

        StringBuilder sb = new StringBuilder();
        sb.append("input=").append(input).append('\n');
        for (IngestOutcome o : IngestOutcome.values()) {
            sb.append("  ingest.").append(o).append('=').append(ingestCounts.get(o).get()).append('\n');
        }
        for (DeliveryStatus s : DeliveryStatus.values()) {
            if (s == DeliveryStatus.BUFFERED) {
                continue;
            }
            sb.append("  delivery.").append(s).append('=').append(deliveryCounts.get(s).get()).append('\n');
        }
        sb.append("  delivery.BUFFERED.gauge=").append(bufferedGauge.get()).append('\n');
        sb.append("  ingest.PENDING_GAP.gauge=").append(pendingGap).append('\n');
        return sb.toString();
    }
}
