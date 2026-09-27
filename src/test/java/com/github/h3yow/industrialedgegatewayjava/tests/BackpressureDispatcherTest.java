package com.github.h3yow.industrialedgegatewayjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.industrialedgegatewayjava.downstream.BackpressureDispatcher;
import com.github.h3yow.industrialedgegatewayjava.downstream.DeadLetterLogger;
import com.github.h3yow.industrialedgegatewayjava.downstream.InMemoryTelemetrySink;
import com.github.h3yow.industrialedgegatewayjava.downstream.RateLimiter;
import com.github.h3yow.industrialedgegatewayjava.downstream.WriteAheadJournal;
import com.github.h3yow.industrialedgegatewayjava.metrics.GatewayMetrics;
import com.github.h3yow.industrialedgegatewayjava.model.DispatchOutcome;
import com.github.h3yow.industrialedgegatewayjava.model.DispatchResult;
import com.github.h3yow.industrialedgegatewayjava.model.Quality;
import com.github.h3yow.industrialedgegatewayjava.model.RawReading;
import com.github.h3yow.industrialedgegatewayjava.model.TelemetryPoint;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景三：下游变慢/不可用。
 * 断言限流、缓存满、死信三种拒绝原因可区分且有上限；ACK 丢失重试不产生重复。
 */
class BackpressureDispatcherTest {

    private static final Logger log = LoggerFactory.getLogger(BackpressureDispatcherTest.class);

    private TelemetryPoint tel(long seq) {
        return new TelemetryPoint("tel-" + seq, "device-A", "temperature", seq, "K",
                Quality.GOOD, 1000L + seq, 2000L + seq, "mapping-v2",
                Map.of("rawPoint", "temp_c"));
    }

    private BackpressureDispatcher dispatcher(InMemoryTelemetrySink sink, RateLimiter limiter,
                                              WriteAheadJournal journal, DeadLetterLogger dl,
                                              GatewayMetrics metrics,
                                              int bufferCapacity, int maxAttempts, long backoffMs) {
        return new BackpressureDispatcher(sink, limiter, journal, dl, metrics,
                bufferCapacity, maxAttempts, backoffMs);
    }

    @Test
    void rateLimitAndBufferFullHaveDistinctReasonsAndCaps() throws Exception {
        InMemoryTelemetrySink sink = new InMemoryTelemetrySink();
        sink.setProcessDelayMs(30); // 下游变慢，让队列堆积
        GatewayMetrics metrics = new GatewayMetrics();
        // 速率极小：第 1 条之后立即触发限流
        RateLimiter limiter = new RateLimiter(0.001d, 1d);
        WriteAheadJournal journal = new WriteAheadJournal(null, new ObjectMapper(), 1000);
        DeadLetterLogger dl = new DeadLetterLogger(null, new ObjectMapper(), 100);
        BackpressureDispatcher d = dispatcher(sink, limiter, journal, dl, metrics, 3, 1, 5);
        d.start();
        try {
            DispatchResult first = d.offer(tel(1));
            log.info("[输入] tel-1 -> {} | {}", first.outcome(), first.detail());
            assertEquals(DispatchOutcome.QUEUED, first.outcome());

            // 无令牌：RATE_LIMITED，依据明确，消息不入队
            DispatchResult limited = d.offer(tel(2));
            log.info("[输入] tel-2 -> {} | {}", limited.outcome(), limited.detail());
            assertEquals(DispatchOutcome.RATE_LIMITED, limited.outcome());

            // 用“不限流”的新投递器验证缓存满路径：下游阻塞 + 队列容量=2
            InMemoryTelemetrySink sink2 = new InMemoryTelemetrySink();
            sink2.setProcessDelayMs(50);
            WriteAheadJournal j2 = new WriteAheadJournal(null, new ObjectMapper(), 1000);
            RateLimiter unlimited = new RateLimiter(1_000_000d, 1_000_000d);
            BackpressureDispatcher d2 = dispatcher(sink2, unlimited, j2, dl, metrics, 2, 1, 5);
            d2.start();
            try {
                DispatchResult q1 = d2.offer(tel(10));
                DispatchResult q2 = d2.offer(tel(11));
                DispatchResult full = d2.offer(tel(12));
                log.info("[输入] 慢下游: tel-10->{} tel-11->{} tel-12->{} | {}",
                        q1.outcome(), q2.outcome(), full.outcome(), full.detail());
                assertEquals(DispatchOutcome.QUEUED, q1.outcome());
                assertEquals(DispatchOutcome.QUEUED, q2.outcome());
                assertEquals(DispatchOutcome.BUFFER_FULL, full.outcome(),
                        "队列达到上限必须以 BUFFER_FULL 明确拒绝");
            } finally {
                d2.stop();
            }
            d.awaitDrained(2000);
        } finally {
            d.stop();
        }

        log.info("[对账] RATE_LIMITED={} BUFFER_FULL={} DELIVERED={}",
                metrics.dispatchCount(DispatchOutcome.RATE_LIMITED),
                metrics.dispatchCount(DispatchOutcome.BUFFER_FULL),
                metrics.dispatchCount(DispatchOutcome.DELIVERED));
        assertTrue(metrics.dispatchCount(DispatchOutcome.RATE_LIMITED) >= 1);
        assertTrue(metrics.dispatchCount(DispatchOutcome.BUFFER_FULL) >= 1);
    }

    @Test
    void unavailableDownstreamRetriesThenDeadLetters() throws Exception {
        InMemoryTelemetrySink sink = new InMemoryTelemetrySink();
        sink.setAvailable(false);
        GatewayMetrics metrics = new GatewayMetrics();
        WriteAheadJournal journal = new WriteAheadJournal(null, new ObjectMapper(), 1000);
        DeadLetterLogger dl = new DeadLetterLogger(null, new ObjectMapper(), 100);
        BackpressureDispatcher d = dispatcher(sink, new RateLimiter(1_000_000, 1_000_000),
                journal, dl, metrics, 10, 3, 5);
        d.start();
        try {
            DispatchResult queued = d.offer(tel(7));
            assertEquals(DispatchOutcome.QUEUED, queued.outcome());
            log.info("[输入] tel-7 下游不可用 已入队，等待重试 3 次...");
            d.awaitDrained(2000);
        } finally {
            d.stop();
        }
        log.info("[判定] tel-7 最终 DEAD_LETTER={} 死信详情数={} 下游落库数={}",
                metrics.dispatchCount(DispatchOutcome.DEAD_LETTER), dl.count(),
                sink.deliveredDistinctCount());
        assertEquals(1, metrics.dispatchCount(DispatchOutcome.DEAD_LETTER));
        assertEquals(1, dl.count());
        assertEquals(0, sink.deliveredDistinctCount(), "不可用期间不应有数据被当成成功");
        assertEquals("重试 3 次仍失败，转入死信", dl.deadLetters().get(0).reason());
    }

    @Test
    void ackLostRedeliveryIsIdempotentAndCountedOnce() throws Exception {
        InMemoryTelemetrySink sink = new InMemoryTelemetrySink();
        TelemetryPoint t = tel(99);
        sink.simulateAckLoss(t.telemetryId()); // 首次投递：已落库但 ACK 丢失

        GatewayMetrics metrics = new GatewayMetrics();
        WriteAheadJournal journal = new WriteAheadJournal(null, new ObjectMapper(), 1000);
        DeadLetterLogger dl = new DeadLetterLogger(null, new ObjectMapper(), 100);
        BackpressureDispatcher d = dispatcher(sink, new RateLimiter(1_000_000, 1_000_000),
                journal, dl, metrics, 10, 3, 5);
        d.start();
        try {
            DispatchResult queued = d.offer(t);
            assertEquals(DispatchOutcome.QUEUED, queued.outcome());
            d.awaitDrained(2000);
        } finally {
            d.stop();
        }
        log.info("[判定] ACK 丢失重投: 下游去重命中次数={} 落库 distinct={} DELIVERED 计数={} 死信={}",
                sink.duplicateDeliveryCount(), sink.deliveredDistinctCount(),
                metrics.dispatchCount(DispatchOutcome.DELIVERED),
                metrics.dispatchCount(DispatchOutcome.DEAD_LETTER));
        assertEquals(1, sink.deliveredDistinctCount(), "重复投递不得产生两条数据");
        assertEquals(1, sink.duplicateDeliveryCount(), "重投必须被下游明确识别为重复");
        assertEquals(1, metrics.dispatchCount(DispatchOutcome.DELIVERED), "按一次成功对账");
        assertEquals(0, metrics.dispatchCount(DispatchOutcome.DEAD_LETTER));
    }
}
