package com.github.h3yow.industrialedgegatewayjava.metrics;

import com.github.h3yow.industrialedgegatewayjava.model.DispatchOutcome;
import com.github.h3yow.industrialedgegatewayjava.model.RejectReason;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 网关对账计数器。所有数量只增不减，按“设备 + 原因/结果”分桶。
 * 不变式：对每台设备，
 * received = accepted + duplicate + stale + badQuality + lostOffline；
 * accepted（映射成功的部分）= delivered + rateLimited + bufferFull + deadLetter + unmapped。
 */
public class GatewayMetrics {

    private final AtomicLong received = new AtomicLong();
    private final Map<RejectReason, AtomicLong> byDecision = new LinkedHashMap<>();
    private final Map<DispatchOutcome, AtomicLong> byDispatch = new LinkedHashMap<>();
    private final AtomicLong unmapped = new AtomicLong();
    private final AtomicLong reconnects = new AtomicLong();
    private final AtomicLong gapCount = new AtomicLong();

    public GatewayMetrics() {
        for (RejectReason r : RejectReason.values()) {
            byDecision.put(r, new AtomicLong());
        }
        for (DispatchOutcome d : DispatchOutcome.values()) {
            byDispatch.put(d, new AtomicLong());
        }
    }

    public void recordReceived() {
        received.incrementAndGet();
    }

    public void recordDecision(RejectReason reason) {
        byDecision.get(reason).incrementAndGet();
    }

    public void recordDispatch(DispatchOutcome outcome) {
        byDispatch.get(outcome).incrementAndGet();
    }

    public void recordUnmapped() {
        unmapped.incrementAndGet();
    }

    public void recordReconnect() {
        reconnects.incrementAndGet();
    }

    public void recordGap() {
        gapCount.incrementAndGet();
    }

    public long received() {
        return received.get();
    }

    public long decisionCount(RejectReason reason) {
        return byDecision.get(reason).get();
    }

    public long dispatchCount(DispatchOutcome outcome) {
        return byDispatch.get(outcome).get();
    }

    public long unmapped() {
        return unmapped.get();
    }

    public long reconnects() {
        return reconnects.get();
    }

    public long gapCount() {
        return gapCount.get();
    }

    /**
     * 输出全部计数快照，便于对账与 /metrics 展示。
     */
    public Map<String, Long> snapshot() {
        Map<String, Long> s = new LinkedHashMap<>();
        s.put("received", received.get());
        byDecision.forEach((k, v) -> s.put("decision." + camelToDot(k.name()), v.get()));
        byDispatch.forEach((k, v) -> s.put("dispatch." + camelToDot(k.name()), v.get()));
        s.put("unmapped", unmapped.get());
        s.put("reconnects", reconnects.get());
        s.put("gaps", gapCount.get());
        return s;
    }

    private static String camelToDot(String name) {
        return name.toLowerCase().replace('_', '.');
    }
}
