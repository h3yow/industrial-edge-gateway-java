package com.github.h3yow.industrialedgegatewayjava.bootstrap;

import com.github.h3yow.industrialedgegatewayjava.delivery.DeliveryService;
import com.github.h3yow.industrialedgegatewayjava.delivery.SlowDownstreamSink;
import com.github.h3yow.industrialedgegatewayjava.ingest.DeviceConnector;
import com.github.h3yow.industrialedgegatewayjava.ingest.IngestionService;
import com.github.h3yow.industrialedgegatewayjava.ingest.SimulatedDevice;
import com.github.h3yow.industrialedgegatewayjava.model.RawReading;
import com.github.h3yow.industrialedgegatewayjava.observability.GatewayMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 本地端到端演示（不连任何外部服务）：
 * <pre>
 * 1. device-A、device-B 同时接入，上报温度/累计流量（两种协议单位）；
 * 2. device-A 断线期间持续产生读数 -> 重连后以重发标记回放（含已确认数据的重复重发）；
 * 3. device-B 乱序上报：先 seq 大后 seq 小，验证缺口缓冲/迟到拒绝/累计不重复；
 * 4. 注入坏质量与越界时间戳，验证异常被明确拒绝而非吞掉；
 * 5. 下游置为不可用/变慢，制造缓存、限流与丢弃；
 * 6. 恢复下游，等待排空，打印设备最终状态与对账报告。
 * </pre>
 * 开启方式：--gateway.demo.enabled=true
 */
@Component
@ConditionalOnProperty(name = "gateway.demo.enabled", havingValue = "true")
public class LocalDemoRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(LocalDemoRunner.class);

    private final IngestionService ingestionService;
    private final SlowDownstreamSink sink;
    private final GatewayMetrics metrics;
    private final DeliveryService deliveryService;

    public LocalDemoRunner(IngestionService ingestionService, SlowDownstreamSink sink,
                           GatewayMetrics metrics, DeliveryService deliveryService) {
        this.ingestionService = ingestionService;
        this.sink = sink;
        this.metrics = metrics;
        this.deliveryService = deliveryService;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("=== local demo start ===");
        long t = System.currentTimeMillis();

        SimulatedDevice deviceA = new SimulatedDevice("device-A", 0);
        SimulatedDevice deviceB = new SimulatedDevice("device-B", 0);
        DeviceConnector connA = new DeviceConnector("device-A");
        DeviceConnector connB = new DeviceConnector("device-B");

        // 1) 同时接入
        connA.connect();
        connB.connect();
        ingestionService.updateConnection("device-A", true);
        ingestionService.updateConnection("device-B", true);

        ingest(connA.receive(deviceA.reading("TEMP_c", 20.0, "C", t, "GOOD")));
        ingest(connA.receive(deviceA.reading("TOTAL_flow_l", 100.0, "L", t + 1, "GOOD")));
        // device-B 使用华氏度/立方米，验证单位归一
        ingest(connB.receive(deviceB.reading("temp_f", 68.0, "F", t, "GOOD")));
        ingest(connB.receive(deviceB.reading("flow_m3", 0.1, "m3", t + 1, "GOOD")));

        // 2) device-A 断线，期间读数留存
        connA.disconnect();
        ingestionService.updateConnection("device-A", false);
        RawReading offline1 = deviceA.reading("TEMP_c", 21.0, "C", t + 1000, "GOOD");
        RawReading offline2 = deviceA.reading("TOTAL_flow_l", 50.0, "L", t + 2000, "GOOD");
        connA.receive(offline1);
        connA.receive(offline2);

        // device-B 在 A 断线期间继续：乱序（先发 seq4 超前、再发 seq3 补缺）+ 异常数据
        RawReading b2 = deviceB.reading("temp_f", 70.0, "F", t + 3000, "GOOD"); // seq2 连续
        ingest(connB.receive(b2));
        RawReading b4 = deviceB.reading("temp_f", 72.0, "F", t + 4000, "GOOD"); // seq4(超前)
        ingest(connB.receive(b4));
        RawReading b3 = deviceB.reading("temp_f", 71.0, "F", t + 5000, "GOOD"); // seq3(补缺)
        ingest(connB.receive(b3));
        // device-B seq4.. 保持连续，异常数据在重连后的 device-A 上注入（见下），避免打断背压演示

        // 3) A 重连并回放：再额外重发一条网关早已确认的 seq0，验证重复不累计
        connA.connect();
        ingestionService.updateConnection("device-A", true);
        for (RawReading r : connA.drainReplayOnReconnect()) {
            ingestionService.ingest(r);
        }
        ingestionService.ingest(deviceA.resend(new RawReading("device-A", "TEMP_c", 20.0, "C",
                t, 0, "GOOD", false)));

        // 异常数据不能被当正常数据吞掉：坏质量、时间戳越界、未知点位（各占 device-A 后续序号）
        ingestionService.ingest(deviceA.reading("TEMP_c", 99.0, "C", t + 3000, "BAD"));
        ingestionService.ingest(deviceA.reading("TEMP_c", 22.0, "C", t - 999_999_999L, "GOOD"));
        ingestionService.ingest(deviceA.reading("UNMAPPED_POINT", 1, "X", t + 3001, "GOOD"));

        // 4) 下游变慢 + 不可用，制造背压。
        //    此时 device-B 下一个序号连续（seq5），读数会直接进入投递层。
        log.info("--- downstream becomes slow then unavailable ---");
        sink.setSendDelayMillis(50);
        sink.setAvailable(false);
        for (int i = 0; i < 40; i++) {
            ingest(connB.receive(deviceB.reading("temp_f", 74 + i % 5, "F",
                    t + 7000L + i, "GOOD")));
        }

        // 5) 恢复并等待排空
        sink.setSendDelayMillis(0);
        sink.setAvailable(true);
        log.info("--- downstream recovered, waiting for drain ---");
        for (int i = 0; i < 100; i++) {
            Thread.sleep(200);
            if (deliveryService.spoolSize() == 0) {
                break;
            }
        }

        log.info("=== device snapshots ===\n{}", ingestionService.snapshots());
        log.info("=== reconcile ===\n{}", metrics.reconcileReport(ingestionService.totalPendingGap()));
        if (deliveryService.spoolSize() != 0) {
            log.warn("spool not empty at demo end (size={}); data is durably retained, not lost",
                    deliveryService.spoolSize());
        }
        log.info("=== local demo done ===");
    }

    private void ingest(RawReading reading) {
        if (reading != null) {
            ingestionService.ingest(reading);
        }
    }
}
