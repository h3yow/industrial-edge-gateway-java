package com.github.h3yow.industrialedgegatewayjava.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.industrialedgegatewayjava.archive.RawArchive;
import com.github.h3yow.industrialedgegatewayjava.downstream.BackpressureDispatcher;
import com.github.h3yow.industrialedgegatewayjava.downstream.DeadLetterLogger;
import com.github.h3yow.industrialedgegatewayjava.downstream.InMemoryTelemetrySink;
import com.github.h3yow.industrialedgegatewayjava.downstream.RateLimiter;
import com.github.h3yow.industrialedgegatewayjava.downstream.WriteAheadJournal;
import com.github.h3yow.industrialedgegatewayjava.ingest.IngestionService;
import com.github.h3yow.industrialedgegatewayjava.mapping.MappingRegistry;
import com.github.h3yow.industrialedgegatewayjava.mapping.MappingVersions;
import com.github.h3yow.industrialedgegatewayjava.mapping.PointMapper;
import com.github.h3yow.industrialedgegatewayjava.metrics.GatewayMetrics;
import com.github.h3yow.industrialedgegatewayjava.session.SessionManager;
import com.github.h3yow.industrialedgegatewayjava.state.DeviceStateStore;
import com.github.h3yow.industrialedgegatewayjava.transform.TelemetryConverter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/**
 * 网关组件装配（全部为本地实现，不连接任何外部服务）。
 */
@Configuration
@EnableConfigurationProperties(GatewayProperties.class)
public class GatewayConfig {

    @Bean
    public GatewayMetrics gatewayMetrics() {
        return new GatewayMetrics();
    }

    @Bean
    public MappingRegistry mappingRegistry() {
        // 当前生效 v2（温度以 K 为基准）；v1 永久保留，供历史数据按旧规则回放复现
        return new MappingRegistry(MappingVersions.V2, MappingVersions.v1(), MappingVersions.v2());
    }

    @Bean
    public PointMapper pointMapper() {
        return new PointMapper();
    }

    @Bean
    public TelemetryConverter telemetryConverter(MappingRegistry registry, PointMapper pointMapper) {
        return new TelemetryConverter(registry, pointMapper);
    }

    @Bean
    public DeviceStateStore deviceStateStore(GatewayProperties props) {
        Path p = Path.of(props.getDataDir(), "device-state.tsv");
        return new DeviceStateStore(p);
    }

    @Bean
    public SessionManager sessionManager(DeviceStateStore store, GatewayProperties props) {
        return new SessionManager(store, props.getSessionMaxPending());
    }

    @Bean
    public RawArchive rawArchive(GatewayProperties props) {
        return new RawArchive(props.getArchiveCapacity());
    }

    @Bean
    public InMemoryTelemetrySink inMemoryTelemetrySink() {
        return new InMemoryTelemetrySink();
    }

    @Bean
    public RateLimiter rateLimiter(GatewayProperties props) {
        return new RateLimiter(props.getRatePermitsPerSecond(), props.getRateBurstCapacity());
    }

    @Bean
    public WriteAheadJournal writeAheadJournal(GatewayProperties props, ObjectMapper objectMapper) {
        Path p = Path.of(props.getDataDir(), "downstream.wal");
        return new WriteAheadJournal(p, objectMapper, props.getWalMaxPending());
    }

    @Bean
    public DeadLetterLogger deadLetterLogger(GatewayProperties props, ObjectMapper objectMapper) {
        Path p = Path.of(props.getDataDir(), "dead-letters.jsonl");
        return new DeadLetterLogger(p, objectMapper, props.getDeadLetterMemoryCapacity());
    }

    @Bean
    public BackpressureDispatcher backpressureDispatcher(InMemoryTelemetrySink sink, RateLimiter rateLimiter,
                                                         WriteAheadJournal journal, DeadLetterLogger deadLetters,
                                                         GatewayMetrics metrics, GatewayProperties props) {
        BackpressureDispatcher d = new BackpressureDispatcher(
                sink, rateLimiter, journal, deadLetters, metrics,
                props.getBufferCapacity(), props.getDeliveryMaxAttempts(),
                props.getDeliveryRetryBackoffMs());
        d.start();
        return d;
    }

    @Bean
    public IngestionService ingestionService(SessionManager sessions, TelemetryConverter converter,
                                             DeviceStateStore stateStore, RawArchive archive,
                                             BackpressureDispatcher dispatcher, GatewayMetrics metrics,
                                             GatewayProperties props) {
        return new IngestionService(sessions, converter, stateStore, archive, dispatcher, metrics,
                props.isFlushStatePerRead());
    }
}
