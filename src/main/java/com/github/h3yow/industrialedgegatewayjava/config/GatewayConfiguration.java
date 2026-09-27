package com.github.h3yow.industrialedgegatewayjava.config;

import com.github.h3yow.industrialedgegatewayjava.delivery.AcknowledgedKeyStore;
import com.github.h3yow.industrialedgegatewayjava.delivery.DeliveryService;
import com.github.h3yow.industrialedgegatewayjava.delivery.FileAcknowledgedKeyStore;
import com.github.h3yow.industrialedgegatewayjava.delivery.SlowDownstreamSink;
import com.github.h3yow.industrialedgegatewayjava.delivery.TokenBucketRateLimiter;
import com.github.h3yow.industrialedgegatewayjava.ingest.IngestionService;
import com.github.h3yow.industrialedgegatewayjava.observability.DecisionLog;
import com.github.h3yow.industrialedgegatewayjava.observability.GatewayMetrics;
import com.github.h3yow.industrialedgegatewayjava.persistence.FileSpool;
import com.github.h3yow.industrialedgegatewayjava.persistence.RawLog;
import com.github.h3yow.industrialedgegatewayjava.persistence.Spool;
import com.github.h3yow.industrialedgegatewayjava.replay.ReplayService;
import com.github.h3yow.industrialedgegatewayjava.transform.MappingRegistry;
import com.github.h3yow.industrialedgegatewayjava.transform.PointMapping;
import com.github.h3yow.industrialedgegatewayjava.transform.TelemetryTransformer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.util.List;

/**
 * 本地边缘网关装配。所有组件均为本地实现，无外部服务、无真实凭据。
 *
 * <p>默认映射规则 v1（详见 docs）：
 * <ul>
 *   <li>device-A：TEMP_c(°C,瞬时) / TOTAL_flow_l(升,累计)</li>
 *   <li>device-B：temp_f(°F,瞬时) / flow_m3(立方米,累计)</li>
 * </ul>
 * 统一指标 temperature_c / total_volume_l，单位统一为 °C / L。
 */
@Configuration
public class GatewayConfiguration {

    @Bean
    public DecisionLog decisionLog() {
        return new DecisionLog();
    }

    @Bean
    public GatewayMetrics gatewayMetrics() {
        return new GatewayMetrics();
    }

    @Bean
    public MappingRegistry mappingRegistry() {
        MappingRegistry registry = new MappingRegistry();
        registry.register("v1", List.of(
                new PointMapping("device-A", "TEMP_c", "temperature_c", "C", 1.0d, 0.0d, false),
                new PointMapping("device-A", "TOTAL_flow_l", "total_volume_l", "L", 1.0d, 0.0d, true),
                new PointMapping("device-B", "temp_f", "temperature_c", "C",
                        5.0d / 9.0d, -32.0d * 5.0d / 9.0d, false),
                new PointMapping("device-B", "flow_m3", "total_volume_l", "L", 1000.0d, 0.0d, true)
        ));
        return registry;
    }

    @Bean
    public TelemetryTransformer telemetryTransformer(MappingRegistry registry) {
        return new TelemetryTransformer(registry);
    }

    @Bean
    public Spool spool(@Value("${gateway.spool.dir:./data/spool}") String dir,
                       @Value("${gateway.spool.capacity:1000}") int capacity) {
        return new FileSpool(Path.of(dir), capacity);
    }

    @Bean
    public AcknowledgedKeyStore acknowledgedKeyStore(
            @Value("${gateway.spool.dir:./data/spool}") String dir) {
        return new FileAcknowledgedKeyStore(Path.of(dir, "ack-keys.log"));
    }

    @Bean
    public RawLog rawLog(@Value("${gateway.rawlog.file:./data/raw/readings.log}") String file) {
        return new RawLog(Path.of(file));
    }

    @Bean
    public SlowDownstreamSink downstreamSink() {
        return new SlowDownstreamSink();
    }

    @Bean
    public TokenBucketRateLimiter rateLimiter(
            @Value("${gateway.rate.permits-per-second:100.0}") double permitsPerSecond,
            @Value("${gateway.rate.burst:200}") int burst) {
        return new TokenBucketRateLimiter(permitsPerSecond, burst);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public DeliveryService deliveryService(SlowDownstreamSink sink, Spool spool,
                                           TokenBucketRateLimiter rateLimiter,
                                           GatewayMetrics metrics, DecisionLog decisionLog,
                                           AcknowledgedKeyStore keyStore) {
        return new DeliveryService(sink, spool, rateLimiter, metrics, decisionLog, keyStore);
    }

    @Bean
    public IngestionService ingestionService(MappingRegistry registry,
                                             TelemetryTransformer transformer,
                                             GatewayMetrics metrics,
                                             DecisionLog decisionLog,
                                             DeliveryService deliveryService,
                                             @Value("${gateway.max-clock-skew-millis:60000}") long skew,
                                             @Value("${gateway.gap-buffer-capacity:64}") int gapCapacity) {
        IngestionService service = new IngestionService(registry, transformer, metrics,
                decisionLog, skew, gapCapacity);
        service.setDeliverySink(event -> {
            deliveryService.accept(event);
            return null;
        });
        return service;
    }

    @Bean
    public ReplayService replayService(TelemetryTransformer transformer) {
        return new ReplayService(transformer);
    }
}
