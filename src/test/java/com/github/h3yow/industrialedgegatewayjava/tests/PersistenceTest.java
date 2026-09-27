package com.github.h3yow.industrialedgegatewayjava.tests;

import com.github.h3yow.industrialedgegatewayjava.delivery.AcknowledgedKeyStore;
import com.github.h3yow.industrialedgegatewayjava.delivery.DeliveryService;
import com.github.h3yow.industrialedgegatewayjava.delivery.FileAcknowledgedKeyStore;
import com.github.h3yow.industrialedgegatewayjava.delivery.InMemoryAcknowledgedKeyStore;
import com.github.h3yow.industrialedgegatewayjava.delivery.SlowDownstreamSink;
import com.github.h3yow.industrialedgegatewayjava.delivery.TokenBucketRateLimiter;
import com.github.h3yow.industrialedgegatewayjava.model.DeliveryStatus;
import com.github.h3yow.industrialedgegatewayjava.model.TelemetryEvent;
import com.github.h3yow.industrialedgegatewayjava.observability.DecisionLog;
import com.github.h3yow.industrialedgegatewayjava.observability.GatewayMetrics;
import com.github.h3yow.industrialedgegatewayjava.persistence.FileSpool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 重启语义：未投递事件从文件 WAL 恢复（不丢失）；已确认键恢复后，
 * 重复投递仍被判为 DUPLICATE_DELIVERY（不翻倍、可辨认）。
 */
class PersistenceTest {

    private static final Logger log = LoggerFactory.getLogger(PersistenceTest.class);

    private TelemetryEvent event(long seq) {
        return new TelemetryEvent("dev", "m", seq, "u", seq, seq, seq, "v1", "dev:" + seq);
    }

    @Test
    void fileSpoolRecoversAcrossRestart(@TempDir Path tempDir) {
        FileSpool spool = new FileSpool(tempDir, 10);
        assertTrue(spool.offer(event(0)));
        assertTrue(spool.offer(event(1)));
        assertEquals(2, spool.size());

        // “重启”：新建实例指向同一目录
        FileSpool recovered = new FileSpool(tempDir, 10);
        List<TelemetryEvent> restored = recovered.poll(10);
        log.info("restored after restart: {}", restored.stream().map(TelemetryEvent::dedupKey).toList());
        assertEquals(2, restored.size());
        assertEquals("dev:0", restored.get(0).dedupKey(), "顺序保持 FIFO");
        assertEquals("dev:1", restored.get(1).dedupKey());
        assertEquals(0, recovered.size(), "poll 后 WAL 被重写清空");

        // 容量上限仍然有效
        FileSpool bounded = new FileSpool(tempDir, 1);
        assertTrue(bounded.offer(event(9)));
        assertFalse(bounded.offer(event(8)), "超过上限必须拒绝，由上层转为可解释终态");
    }

    @Test
    void duplicateRedeliveryAfterRestartIsIdentifiable(@TempDir Path tempDir) {
        Path keyFile = tempDir.resolve("ack.log");
        SlowDownstreamSink sink = new SlowDownstreamSink();
        GatewayMetrics metrics = new GatewayMetrics();
        DecisionLog decisions = new DecisionLog();

        // 重启前：投递 seq0..2 成功，键持久化
        AcknowledgedKeyStore keysBefore = new FileAcknowledgedKeyStore(keyFile);
        DeliveryService before = new DeliveryService(sink, new FileSpool(tempDir.resolve("s1"), 10),
                new TokenBucketRateLimiter(1_000_000, 1_000_000), metrics, decisions, keysBefore);
        for (long s = 0; s < 3; s++) {
            assertEquals(DeliveryStatus.DELIVERED, before.accept(event(s)).status());
        }
        assertEquals(3, sink.acknowledgedCount());

        // “重启”：全新 DeliveryService + 全新 sink（模拟下游也重启），从文件恢复键
        AcknowledgedKeyStore keysAfter = new FileAcknowledgedKeyStore(keyFile);
        SlowDownstreamSink freshSink = new SlowDownstreamSink();
        DeliveryService after = new DeliveryService(freshSink, new FileSpool(tempDir.resolve("s2"), 10),
                new TokenBucketRateLimiter(1_000_000, 1_000_000), new GatewayMetrics(), decisions,
                keysAfter);

        for (long s = 0; s < 3; s++) {
            var receipt = after.accept(event(s));
            log.info("post-restart redelivery seq={} => {}", s, receipt.status());
            assertEquals(DeliveryStatus.DUPLICATE_DELIVERY, receipt.status(),
                    "重启前已确认的事件重复投递必须可辨认，不再打下游");
        }
        assertEquals(0, freshSink.acknowledgedCount(), "重复投递一条都不能再到下游");
        assertEquals(3, after.acknowledgedKeys());
    }
}
