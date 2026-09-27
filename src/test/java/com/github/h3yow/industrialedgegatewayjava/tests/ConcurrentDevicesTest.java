package com.github.h3yow.industrialedgegatewayjava.tests;

import com.github.h3yow.industrialedgegatewayjava.delivery.DeliveryService;
import com.github.h3yow.industrialedgegatewayjava.delivery.InMemoryAcknowledgedKeyStore;
import com.github.h3yow.industrialedgegatewayjava.delivery.SlowDownstreamSink;
import com.github.h3yow.industrialedgegatewayjava.delivery.TokenBucketRateLimiter;
import com.github.h3yow.industrialedgegatewayjava.ingest.IngestionService;
import com.github.h3yow.industrialedgegatewayjava.ingest.SimulatedDevice;
import com.github.h3yow.industrialedgegatewayjava.model.DeviceSnapshot;
import com.github.h3yow.industrialedgegatewayjava.model.IngestOutcome;
import com.github.h3yow.industrialedgegatewayjava.observability.DecisionLog;
import com.github.h3yow.industrialedgegatewayjava.observability.GatewayMetrics;
import com.github.h3yow.industrialedgegatewayjava.persistence.InMemorySpool;
import com.github.h3yow.industrialedgegatewayjava.transform.MappingRegistry;
import com.github.h3yow.industrialedgegatewayjava.transform.PointMapping;
import com.github.h3yow.industrialedgegatewayjava.transform.TelemetryTransformer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多台设备并发上报：每台设备的最终读数/累计状态必须独立一致，输入计数守恒。
 */
class ConcurrentDevicesTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentDevicesTest.class);

    @Test
    void multipleDevicesPublishConcurrentlyWithIndependentConsistentState() throws Exception {
        MappingRegistry registry = new MappingRegistry();
        registry.register("v1", List.of(
                new PointMapping("device-A", "TOTAL_flow_l", "total_volume_l", "L", 1, 0, true),
                new PointMapping("device-B", "TOTAL_flow_l", "total_volume_l", "L", 1, 0, true),
                new PointMapping("device-C", "TOTAL_flow_l", "total_volume_l", "L", 1, 0, true)
        ));
        TelemetryTransformer transformer = new TelemetryTransformer(registry);
        GatewayMetrics metrics = new GatewayMetrics();
        DecisionLog decisions = new DecisionLog();
        DeliveryService delivery = new DeliveryService(new SlowDownstreamSink(), new InMemorySpool(100_000),
                new TokenBucketRateLimiter(1_000_000, 1_000_000), metrics, decisions,
                new InMemoryAcknowledgedKeyStore());
        IngestionService ingestion = new IngestionService(registry, transformer, metrics,
                decisions, 60_000, 128);
        ingestion.setDeliverySink(e -> {
            delivery.accept(e);
            return null;
        });

        int devices = 3;
        int perDevice = 200;
        ExecutorService pool = Executors.newFixedThreadPool(devices);
        CountDownLatch start = new CountDownLatch(1);
        for (int d = 0; d < devices; d++) {
            String deviceId = "device-" + (char) ('A' + d);
            pool.submit(() -> {
                start.await();
                SimulatedDevice device = new SimulatedDevice(deviceId, 0);
                for (int i = 0; i < perDevice; i++) {
                    ingestion.ingest(device.reading("TOTAL_flow_l", 1.0, "L",
                            System.currentTimeMillis(), "GOOD"));
                }
                return null;
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        for (char d = 'A'; d <= 'C'; d++) {
            DeviceSnapshot snap = ingestion.snapshots().get("device-" + d);
            log.info("{} => watermark={} total={}", snap.deviceId(), snap.highWatermark(),
                    snap.totals().get("total_volume_l"));
            assertEquals(perDevice - 1L, snap.highWatermark(), "水位必须连续推进到末尾");
            assertEquals((double) perDevice, snap.totals().get("total_volume_l"), 1e-9,
                    "并发下每台设备恰好累计 200 次，不能因并发重复/遗漏");
        }
        assertEquals((long) devices * perDevice, metrics.totalInput());
        assertEquals((long) devices * perDevice, metrics.ingest(IngestOutcome.ACCEPTED));
        assertEquals((long) devices * perDevice, delivery.acknowledgedKeys(),
                "所有事件且仅一次被下游确认");
        log.info("RECONCILE:\n{}", metrics.reconcileReport(ingestion.totalPendingGap()));
    }
}
