package com.github.h3yow.industrialedgegatewayjava.delivery;

import com.github.h3yow.industrialedgegatewayjava.model.TelemetryEvent;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 可控模拟下游。
 *
 * <ul>
 *   <li>{@link #setSendDelayMillis}：模拟消费变慢，send 会阻塞；</li>
 *   <li>{@link #setAvailable}：模拟暂时不可用，send 抛 {@link DownstreamUnavailableException}；</li>
 *   <li>内部记录已确认幂等键：同一 dedupKey 重复 send 抛
 *       {@link DuplicateDeliveryException}，模拟具备幂等能力的下游。</li>
 * </ul>
 */
public class SlowDownstreamSink implements DownstreamSink {

    /** 下游不可用时抛出（可重试性质的失败）。 */
    public static class DownstreamUnavailableException extends RuntimeException {
        public DownstreamUnavailableException(String message) {
            super(message);
        }
    }

    /** 下游检测到重复幂等键时抛出（不可重试，直接终态）。 */
    public static class DuplicateDeliveryException extends RuntimeException {
        public DuplicateDeliveryException(String message) {
            super(message);
        }
    }

    private volatile boolean available = true;
    private volatile long sendDelayMillis = 0;
    private final Set<String> acknowledged = ConcurrentHashMap.newKeySet();

    public SlowDownstreamSink() {
    }

    public void setAvailable(boolean available) {
        this.available = available;
    }

    public void setSendDelayMillis(long sendDelayMillis) {
        this.sendDelayMillis = sendDelayMillis;
    }

    public int acknowledgedCount() {
        return acknowledged.size();
    }

    @Override
    public void send(TelemetryEvent event) throws InterruptedException {
        if (sendDelayMillis > 0) {
            Thread.sleep(sendDelayMillis);
        }
        if (!available) {
            throw new DownstreamUnavailableException("downstream marked unavailable");
        }
        if (!acknowledged.add(event.dedupKey())) {
            throw new DuplicateDeliveryException("duplicate dedupKey: " + event.dedupKey());
        }
    }

    @Override
    public boolean available() {
        return available;
    }
}
