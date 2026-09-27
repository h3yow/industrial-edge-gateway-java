package com.github.h3yow.industrialedgegatewayjava.delivery;

import com.github.h3yow.industrialedgegatewayjava.model.DeliveryReceipt;
import com.github.h3yow.industrialedgegatewayjava.model.DeliveryStatus;
import com.github.h3yow.industrialedgegatewayjava.model.TelemetryEvent;
import com.github.h3yow.industrialedgegatewayjava.observability.DecisionLog;
import com.github.h3yow.industrialedgegatewayjava.observability.GatewayMetrics;
import com.github.h3yow.industrialedgegatewayjava.persistence.Spool;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * 下游投递编排。
 *
 * <p>每条进入投递层的事件有且仅有一个终态/暂态，且判定依据互相可区分：
 * <ol>
 *   <li>{@code DUPLICATE_DELIVERY}：幂等键已被本网关确认过（重启后从持久化
 *       已确认集合恢复），重复投递不会造成下游重复；</li>
 *   <li>{@code DELIVERED}：拿到限流令牌且下游确认；</li>
 *   <li>{@code BUFFERED}：未获令牌或下游不可用时进入有界缓存，后台 worker 重投，
 *       缓存（文件 WAL）在重启后恢复；</li>
 *   <li>{@code RATE_LIMITED}：限流不足且缓存已满；</li>
 *   <li>{@code DOWNSTREAM_DROPPED}：下游明确不可用且缓存已满。</li>
 * </ol>
 * “缓存满”是唯一的拒绝硬上限；限流与下游可用性决定走缓存时的具体原因。
 */
public class DeliveryService {

    private final DownstreamSink sink;
    private final Spool spool;
    private final TokenBucketRateLimiter rateLimiter;
    private final GatewayMetrics metrics;
    private final DecisionLog decisionLog;
    private final Set<String> deliveredKeys = ConcurrentHashMap.newKeySet();
    private final AcknowledgedKeyStore keyStore;

    private ScheduledExecutorService worker;
    private volatile boolean running;
    private int drainBatchSize = 16;
    private long drainIntervalMillis = 100;

    public DeliveryService(DownstreamSink sink,
                           Spool spool,
                           TokenBucketRateLimiter rateLimiter,
                           GatewayMetrics metrics,
                           DecisionLog decisionLog,
                           AcknowledgedKeyStore keyStore) {
        this.sink = sink;
        this.spool = spool;
        this.rateLimiter = rateLimiter;
        this.metrics = metrics;
        this.decisionLog = decisionLog;
        this.keyStore = keyStore;
        this.deliveredKeys.addAll(keyStore.load());
    }

    /**
     * 接收一条采集成功的事件。
     */
    public DeliveryReceipt accept(TelemetryEvent event) {
        String key = event.dedupKey();
        if (deliveredKeys.contains(key)) {
            metrics.recordDelivery(DeliveryStatus.DUPLICATE_DELIVERY);
            decisionLog.log("delivery", event.deviceId(), event.sequence(),
                    "DUPLICATE_DELIVERY", null, "already acknowledged; downstream not called");
            return DeliveryReceipt.duplicate(key);
        }

        if (rateLimiter.tryAcquire()) {
            DeliveryReceipt direct = tryDirect(event);
            if (direct != null) {
                return direct;
            }
            // 下游不可用：依据为 downstream，不是 rate limit
            return toSpool(event, false);
        }
        // 无令牌：限流路径
        return toSpool(event, true);
    }

    private DeliveryReceipt tryDirect(TelemetryEvent event) {
        try {
            sink.send(event);
            markDelivered(event);
            metrics.recordDelivery(DeliveryStatus.DELIVERED);
            decisionLog.log("delivery", event.deviceId(), event.sequence(), "DELIVERED", null,
                    "metric=" + event.metricName() + " downstream ack");
            return DeliveryReceipt.delivered(event.dedupKey());
        } catch (SlowDownstreamSink.DuplicateDeliveryException e) {
            // 下游（或重启后的下游状态）已知该键：记为幂等成功，不重试。
            markDelivered(event);
            metrics.recordDelivery(DeliveryStatus.DUPLICATE_DELIVERY);
            decisionLog.log("delivery", event.deviceId(), event.sequence(),
                    "DUPLICATE_DELIVERY", null, "downstream reported duplicate key");
            return DeliveryReceipt.duplicate(event.dedupKey());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (RuntimeException e) {
            // 下游不可用/失败：转为缓存重试。
            decisionLog.log("delivery", event.deviceId(), event.sequence(),
                    "DOWNSTREAM_FAIL", null, e.getMessage() + "; will spool");
            return null;
        }
    }

    private DeliveryReceipt toSpool(TelemetryEvent event, boolean rateLimitedReason) {
        if (spool.offer(event)) {
            metrics.setBufferedGauge(spool.size());
            decisionLog.log("delivery", event.deviceId(), event.sequence(), "BUFFERED", null,
                    "spooled (" + (rateLimitedReason ? "rate limited" : "downstream unavailable")
                            + "), spoolSize=" + spool.size() + "/" + spool.capacity());
            return DeliveryReceipt.buffered(event.dedupKey());
        }
        if (rateLimitedReason) {
            metrics.recordDelivery(DeliveryStatus.RATE_LIMITED);
            decisionLog.log("delivery", event.deviceId(), event.sequence(), "RATE_LIMITED", null,
                    "no token and spool full " + spool.size() + "/" + spool.capacity());
            return DeliveryReceipt.rateLimited(event.dedupKey());
        }
        metrics.recordDelivery(DeliveryStatus.DOWNSTREAM_DROPPED);
        decisionLog.log("delivery", event.deviceId(), event.sequence(), "DOWNSTREAM_DROPPED",
                "DOWNSTREAM_UNAVAILABLE",
                "downstream unavailable and spool full " + spool.size() + "/" + spool.capacity());
        return DeliveryReceipt.dropped(event.dedupKey());
    }

    private void markDelivered(TelemetryEvent event) {
        deliveredKeys.add(event.dedupKey());
        keyStore.append(event.dedupKey());
    }

    /** 后台排空一轮：从缓存取一批，逐条直投；仍不可投递的按原顺序整体放回队首，稍后重试。 */
    public synchronized void drainNow() {
        List<TelemetryEvent> batch = spool.poll(drainBatchSize);
        if (batch.isEmpty()) {
            return;
        }
        List<TelemetryEvent> requeue = new java.util.ArrayList<>();
        for (TelemetryEvent event : batch) {
            String key = event.dedupKey();
            if (deliveredKeys.contains(key)) {
                metrics.recordDelivery(DeliveryStatus.DUPLICATE_DELIVERY);
                continue;
            }
            if (rateLimiter.tryAcquire()) {
                DeliveryReceipt direct = tryDirect(event);
                if (direct != null) {
                    continue;
                }
            }
            requeue.add(event);
        }
        if (!requeue.isEmpty()) {
            // 刚 poll 出这批，队列有确定空间，prepend 不会失败 -> 不丢数据、保持顺序。
            spool.prepend(requeue);
            decisionLog.log("delivery", requeue.get(0).deviceId(), requeue.get(0).sequence(),
                    "SPOOL_REQUEUE", null,
                    requeue.size() + " event(s) still undeliverable, returned to spool head");
        }
        metrics.setBufferedGauge(spool.size());
    }

    public void start() {
        if (running) {
            return;
        }
        running = true;
        worker = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "spool-drainer");
            t.setDaemon(true);
            return t;
        });
        worker.scheduleWithFixedDelay(this::drainNow, drainIntervalMillis, drainIntervalMillis,
                TimeUnit.MILLISECONDS);
    }

    public void stop() {
        running = false;
        if (worker != null) {
            worker.shutdownNow();
        }
    }

    public int spoolSize() {
        return spool.size();
    }

    public int acknowledgedKeys() {
        return deliveredKeys.size();
    }

    /** 某个幂等键是否已被确认投递（测试/对账使用）。 */
    public boolean isAcknowledged(String dedupKey) {
        return deliveredKeys.contains(dedupKey);
    }

    public void configureDrain(int batchSize, long intervalMillis) {
        this.drainBatchSize = batchSize;
        this.drainIntervalMillis = intervalMillis;
    }
}
