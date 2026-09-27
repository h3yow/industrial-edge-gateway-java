package com.github.h3yow.industrialedgegatewayjava.tests;

import com.github.h3yow.industrialedgegatewayjava.delivery.DeliveryService;
import com.github.h3yow.industrialedgegatewayjava.delivery.InMemoryAcknowledgedKeyStore;
import com.github.h3yow.industrialedgegatewayjava.delivery.SlowDownstreamSink;
import com.github.h3yow.industrialedgegatewayjava.delivery.TokenBucketRateLimiter;
import com.github.h3yow.industrialedgegatewayjava.model.DeliveryReceipt;
import com.github.h3yow.industrialedgegatewayjava.model.DeliveryStatus;
import com.github.h3yow.industrialedgegatewayjava.model.TelemetryEvent;
import com.github.h3yow.industrialedgegatewayjava.observability.DecisionLog;
import com.github.h3yow.industrialedgegatewayjava.observability.GatewayMetrics;
import com.github.h3yow.industrialedgegatewayjava.persistence.InMemorySpool;
import com.github.h3yow.industrialedgegatewayjava.persistence.Spool;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景三：下游消费变慢/暂时不可用。
 *
 * 验证：
 * 1. 缓存（BUFFERED）有明确上限；
 * 2. 缓存满后，“限流导致”与“下游不可用导致”的拒绝原因可区分
 *    （RATE_LIMITED vs DOWNSTREAM_DROPPED）；
 * 3. 下游恢复后缓存排空，无静默丢失；
 * 4. 重复投递得到 DUPLICATE_DELIVERY，下游不会收到两次。
 */
class DownstreamBackpressureTest {

    private static final Logger log = LoggerFactory.getLogger(DownstreamBackpressureTest.class);

    private TelemetryEvent event(long seq) {
        return new TelemetryEvent("dev", "m", seq, "u", seq, seq, seq, "v1", "dev:" + seq);
    }

    @Test
    void rateLimitedAndDownstreamUnavailableHaveDistinctReasons() {
        SlowDownstreamSink sink = new SlowDownstreamSink();
        Spool spool = new InMemorySpool(3);
        // 极慢的令牌桶：初始桶容量 1，仅第一次直投有令牌
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(0.0001d, 1);
        GatewayMetrics metrics = new GatewayMetrics();
        DecisionLog decisions = new DecisionLog();
        DeliveryService service = new DeliveryService(sink, spool, limiter, metrics, decisions,
                new InMemoryAcknowledgedKeyStore());

        // seq0：拿到唯一令牌，直投成功
        DeliveryReceipt r0 = service.accept(event(0));
        assertEquals(DeliveryStatus.DELIVERED, r0.status());

        // seq1..3：无令牌 -> 缓存（容量 3）
        for (long s = 1; s <= 3; s++) {
            DeliveryReceipt r = service.accept(event(s));
            log.info("INPUT seq={} => {} spool={}/{}", s, r.status(), spool.size(), spool.capacity());
            assertEquals(DeliveryStatus.BUFFERED, r.status());
        }
        assertEquals(3, service.spoolSize());

        // seq4：无令牌且缓存满 => RATE_LIMITED（依据：限流）
        DeliveryReceipt r4 = service.accept(event(4));
        log.info("INPUT seq=4 => {} reason='{}'", r4.status(), r4.reason());
        assertEquals(DeliveryStatus.RATE_LIMITED, r4.status());

        // 下游不可用 + 人为给一个令牌的场景不可控；直接切下游不可用，
        // 等待令牌补充后投递会走“下游失败 -> 缓存失败”路径。
        sink.setAvailable(false);
        TokenBucketRateLimiter freshLimiter = new TokenBucketRateLimiter(1_000_000, 1_000_000);
        DeliveryService service2 = new DeliveryService(sink, spool, freshLimiter, metrics, decisions,
                new InMemoryAcknowledgedKeyStore());
        DeliveryReceipt r5 = service2.accept(event(5));
        log.info("INPUT seq=5 with downstream unavailable & full spool => {} reason='{}'",
                r5.status(), r5.reason());
        assertEquals(DeliveryStatus.DOWNSTREAM_DROPPED, r5.status());
        assertTrue(r5.reason().contains("unavailable"));

        decisions.dump("dev");
        log.info("spool size at pressure peak: {}", service.spoolSize());
        assertTrue(service.spoolSize() <= spool.capacity(), "缓存必须遵守硬上限");
    }

    @Test
    void bufferedEventsAreDeliveredAfterRecoveryWithoutLossOrDuplication() throws Exception {
        SlowDownstreamSink sink = new SlowDownstreamSink();
        Spool spool = new InMemorySpool(10);
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1_000_000, 1_000_000);
        GatewayMetrics metrics = new GatewayMetrics();
        DecisionLog decisions = new DecisionLog();
        DeliveryService service = new DeliveryService(sink, spool, limiter, metrics, decisions,
                new InMemoryAcknowledgedKeyStore());
        service.configureDrain(10, 20);

        // 下游不可用时压入 5 条 -> 全部缓存
        sink.setAvailable(false);
        for (long s = 0; s < 5; s++) {
            assertEquals(DeliveryStatus.BUFFERED, service.accept(event(s)).status());
        }
        assertEquals(5, service.spoolSize());
        log.info("downstream unavailable: 5 events buffered, none delivered, ack={}",
                sink.acknowledgedCount());

        // 恢复并启动 worker，等待排空
        sink.setAvailable(true);
        service.start();
        long deadline = System.currentTimeMillis() + 5_000;
        while (service.spoolSize() > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(25);
        }
        service.stop();
        assertEquals(0, service.spoolSize(), "恢复后缓存必须排空，不丢数据");
        assertEquals(5, sink.acknowledgedCount(), "5 条全部且仅一次被下游确认");

        // 重复投递：seq0..4 重放给投递层 -> 全部 DUPLICATE_DELIVERY，下游计数不变
        for (long s = 0; s < 5; s++) {
            DeliveryReceipt r = service.accept(event(s));
            assertEquals(DeliveryStatus.DUPLICATE_DELIVERY, r.status(),
                    "重复投递必须可辨认，不能再次打到下游");
        }
        assertEquals(5, sink.acknowledgedCount(), "重复投递不能让下游重复收到");
        decisions.dump("dev");
    }
}
