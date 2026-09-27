package com.github.h3yow.industrialedgegatewayjava.web;

import com.github.h3yow.industrialedgegatewayjava.archive.RawArchive;
import com.github.h3yow.industrialedgegatewayjava.downstream.BackpressureDispatcher;
import com.github.h3yow.industrialedgegatewayjava.downstream.DeadLetterLogger;
import com.github.h3yow.industrialedgegatewayjava.downstream.InMemoryTelemetrySink;
import com.github.h3yow.industrialedgegatewayjava.ingest.IngestionService;
import com.github.h3yow.industrialedgegatewayjava.mapping.MappingRegistry;
import com.github.h3yow.industrialedgegatewayjava.metrics.GatewayMetrics;
import com.github.h3yow.industrialedgegatewayjava.model.IngestResult;
import com.github.h3yow.industrialedgegatewayjava.model.Quality;
import com.github.h3yow.industrialedgegatewayjava.model.RawReading;
import com.github.h3yow.industrialedgegatewayjava.model.TelemetryPoint;
import com.github.h3yow.industrialedgegatewayjava.session.SessionManager;
import com.github.h3yow.industrialedgegatewayjava.sim.SimulatorRunner;
import com.github.h3yow.industrialedgegatewayjava.state.DeviceStateStore;
import com.github.h3yow.industrialedgegatewayjava.transform.TelemetryConverter;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 本地运维/验证接口（仅绑定本地，无外部依赖）。
 */
@RestController
@RequestMapping("/api")
public class GatewayController {

    private final IngestionService ingestion;
    private final SessionManager sessions;
    private final DeviceStateStore stateStore;
    private final RawArchive archive;
    private final TelemetryConverter converter;
    private final MappingRegistry mappingRegistry;
    private final BackpressureDispatcher dispatcher;
    private final InMemoryTelemetrySink sink;
    private final DeadLetterLogger deadLetters;
    private final GatewayMetrics metrics;
    private final SimulatorRunner simulator;

    public GatewayController(IngestionService ingestion, SessionManager sessions, DeviceStateStore stateStore,
                             RawArchive archive, TelemetryConverter converter, MappingRegistry mappingRegistry,
                             BackpressureDispatcher dispatcher, InMemoryTelemetrySink sink,
                             DeadLetterLogger deadLetters, GatewayMetrics metrics, SimulatorRunner simulator) {
        this.ingestion = ingestion;
        this.sessions = sessions;
        this.stateStore = stateStore;
        this.archive = archive;
        this.converter = converter;
        this.mappingRegistry = mappingRegistry;
        this.dispatcher = dispatcher;
        this.sink = sink;
        this.deadLetters = deadLetters;
        this.metrics = metrics;
        this.simulator = simulator;
    }

    /** 原始上报入站请求体。 */
    public record IngestRequest(String deviceId, Long sequence, String pointId, Double value,
                                String unit, String quality, Long deviceTimeMs,
                                Boolean connect) {
    }

    @PostMapping("/devices/{deviceId}/connect")
    public Map<String, Object> connect(@PathVariable String deviceId) {
        boolean reconnect = sessions.connect(deviceId);
        if (reconnect) {
            metrics.recordReconnect();
        }
        return Map.of("deviceId", deviceId, "reconnected", reconnect);
    }

    @PostMapping("/devices/{deviceId}/disconnect")
    public Map<String, Object> disconnect(@PathVariable String deviceId) {
        sessions.disconnect(deviceId);
        return Map.of("deviceId", deviceId, "status", "DISCONNECTED");
    }

    /**
     * 上报一条原始读数。返回判定依据、转换结果与投递结论。
     */
    @PostMapping("/ingest")
    public List<IngestResult> ingest(@RequestBody IngestRequest req) {
        if (req.deviceId() == null || req.sequence() == null || req.pointId() == null
                || req.value() == null || req.unit() == null) {
            throw new IllegalArgumentException("deviceId/sequence/pointId/value/unit 必填");
        }
        if (Boolean.TRUE.equals(req.connect())) {
            sessions.connect(req.deviceId());
        }
        Quality q = req.quality() == null ? Quality.GOOD : Quality.valueOf(req.quality());
        long eventTime = req.deviceTimeMs() != null ? req.deviceTimeMs() : System.currentTimeMillis();
        RawReading reading = new RawReading(req.deviceId(), req.sequence(), req.pointId(),
                req.value(), req.unit(), q, eventTime);
        return ingestion.ingestBatch(reading);
    }

    @GetMapping("/state")
    public Map<String, Object> state() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("sequences", stateStore.sequenceSnapshot());
        body.put("accumulators", describeAccumulators());
        return body;
    }

    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        Map<String, Object> body = new LinkedHashMap<>(metrics.snapshot());
        body.put("queueDepth", dispatcher.queueDepth());
        body.put("downstreamDistinctDelivered", sink.deliveredDistinctCount());
        body.put("downstreamDuplicateDeliveries", sink.duplicateDeliveryCount());
        body.put("deadLetters", deadLetters.count());
        return body;
    }

    @GetMapping("/telemetry")
    public List<TelemetryPoint> telemetry() {
        // 内存 sink 不提供列表接口，这里返回最近归档原始读数的当前版本转换结果用于观察
        return archive.replay().stream()
                .map(r -> converter.convert(r, r.deviceTimeMs()).orElse(null))
                .filter(java.util.Objects::nonNull)
                .toList();
    }

    /** 按指定映射版本回放全部归档原始读数（不传版本则用当前版本）。同输入同版本结果确定可复现。 */
    @GetMapping("/replay")
    public List<TelemetryPoint> replay(@RequestParam(required = false) String version) {
        String v = version != null ? version : mappingRegistry.currentVersionName();
        return archive.replay().stream()
                .map(r -> converter.convert(r, r.deviceTimeMs(), v).orElse(null))
                .filter(java.util.Objects::nonNull)
                .toList();
    }

    @GetMapping("/mappings")
    public Map<String, Object> mappings() {
        return Map.of("current", mappingRegistry.currentVersionName(),
                "versions", mappingRegistry.versionNames());
    }

    @GetMapping("/dead-letters")
    public List<DeadLetterLogger.DeadLetter> deadLetters() {
        return deadLetters.deadLetters();
    }

    @PostMapping("/simulator/{deviceId}/disconnect")
    public Map<String, Object> simDisconnect(@PathVariable String deviceId) {
        simulator.disconnect(deviceId);
        return Map.of("deviceId", deviceId, "action", "disconnect");
    }

    @PostMapping("/simulator/{deviceId}/reconnect")
    public Map<String, Object> simReconnect(@PathVariable String deviceId) {
        simulator.reconnect(deviceId);
        return Map.of("deviceId", deviceId, "action", "reconnect");
    }

    @PostMapping("/downstream/available")
    public Map<String, Object> downstreamAvailable(@RequestParam boolean available) {
        sink.setAvailable(available);
        return Map.of("available", available);
    }

    @PostMapping("/downstream/delay")
    public Map<String, Object> downstreamDelay(@RequestParam long ms) {
        sink.setProcessDelayMs(ms);
        return Map.of("processDelayMs", ms);
    }

    private Map<String, Map<String, Map<String, Object>>> describeAccumulators() {
        Map<String, Map<String, Map<String, Object>>> out = new LinkedHashMap<>();
        stateStore.snapshot().forEach((dev, points) -> {
            Map<String, Map<String, Object>> devMap = new LinkedHashMap<>();
            points.forEach((metric, acc) -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("count", acc.count());
                m.put("sum", acc.sum());
                m.put("min", acc.min());
                m.put("max", acc.max());
                m.put("average", acc.average());
                m.put("lastValue", acc.lastValue());
                m.put("lastEventTimeMs", acc.lastEventTimeMs());
                m.put("lastSequence", acc.lastSequence());
                devMap.put(metric, m);
            });
            out.put(dev, devMap);
        });
        return out;
    }
}
