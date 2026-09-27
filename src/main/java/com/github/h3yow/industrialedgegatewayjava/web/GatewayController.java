package com.github.h3yow.industrialedgegatewayjava.web;

import com.github.h3yow.industrialedgegatewayjava.delivery.DeliveryService;
import com.github.h3yow.industrialedgegatewayjava.delivery.SlowDownstreamSink;
import com.github.h3yow.industrialedgegatewayjava.ingest.IngestionService;
import com.github.h3yow.industrialedgegatewayjava.model.DeviceSnapshot;
import com.github.h3yow.industrialedgegatewayjava.model.IngestResult;
import com.github.h3yow.industrialedgegatewayjava.model.RawReading;
import com.github.h3yow.industrialedgegatewayjava.observability.DecisionLog;
import com.github.h3yow.industrialedgegatewayjava.observability.GatewayMetrics;
import com.github.h3yow.industrialedgegatewayjava.persistence.RawLog;
import com.github.h3yow.industrialedgegatewayjava.replay.ReplayService;
import com.github.h3yow.industrialedgegatewayjava.transform.MappingRegistry;
import org.springframework.web.bind.annotation.ExceptionHandler;
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
 * 本地观测与操作接口（无外部服务依赖）。
 *
 * <ul>
 *   <li>POST /api/ingest            手工上报一条原始读数</li>
 *   <li>GET  /api/devices           每台设备的最终读数与累计状态</li>
 *   <li>GET  /api/metrics           分桶计数</li>
 *   <li>GET  /api/reconcile         对账报告（含恒等式校验）</li>
 *   <li>GET  /api/decisions/{id}    某设备判定链路</li>
 *   <li>GET  /api/replay            用指定映射版本回放留存的原始读数</li>
 *   <li>POST /api/downstream/state  切换下游可用/不可用（演示/验证背压）</li>
 * </ul>
 */
@RestController
@RequestMapping("/api")
public class GatewayController {

    private final IngestionService ingestionService;
    private final DeliveryService deliveryService;
    private final GatewayMetrics metrics;
    private final DecisionLog decisionLog;
    private final MappingRegistry mappingRegistry;
    private final ReplayService replayService;
    private final RawLog rawLog;
    private final SlowDownstreamSink sink;

    public GatewayController(IngestionService ingestionService,
                             DeliveryService deliveryService,
                             GatewayMetrics metrics,
                             DecisionLog decisionLog,
                             MappingRegistry mappingRegistry,
                             ReplayService replayService,
                             RawLog rawLog,
                             SlowDownstreamSink sink) {
        this.ingestionService = ingestionService;
        this.deliveryService = deliveryService;
        this.metrics = metrics;
        this.decisionLog = decisionLog;
        this.mappingRegistry = mappingRegistry;
        this.replayService = replayService;
        this.rawLog = rawLog;
        this.sink = sink;
    }

    @PostMapping("/ingest")
    public Map<String, Object> ingest(@RequestBody RawReading reading) {
        rawLog.append(reading);
        IngestResult result = ingestionService.ingest(reading);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("outcome", result.outcome());
        if (result.reason() != null) {
            out.put("reason", result.reason());
        }
        if (result.event() != null) {
            out.put("event", result.event());
        }
        out.put("spoolSize", deliveryService.spoolSize());
        return out;
    }

    @GetMapping("/devices")
    public Map<String, DeviceSnapshot> devices() {
        return ingestionService.snapshots();
    }

    @GetMapping("/metrics")
    public Map<String, Long> metrics() {
        return metrics.snapshot();
    }

    @GetMapping("/reconcile")
    public Map<String, Object> reconcile() {
        long pendingGap = ingestionService.totalPendingGap();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("report", metrics.reconcileReport(pendingGap));
        out.put("spoolSize", deliveryService.spoolSize());
        return out;
    }

    @GetMapping("/decisions/{deviceId}")
    public String decisions(@PathVariable String deviceId) {
        return decisionLog.dump(deviceId);
    }

    @GetMapping("/replay")
    public List<ReplayService.ReplayLine> replay(@RequestParam(required = false) String version) {
        String v = version != null ? version : mappingRegistry.currentVersion();
        return replayService.replayExplained(rawLog.readAll(), v);
    }

    @PostMapping("/downstream/state")
    public Map<String, Object> downstreamState(@RequestParam boolean available,
                                               @RequestParam(required = false, defaultValue = "0") long delayMillis) {
        sink.setAvailable(available);
        sink.setSendDelayMillis(delayMillis);
        return Map.of("available", available, "sendDelayMillis", delayMillis);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public Map<String, String> badRequest(IllegalArgumentException e) {
        return Map.of("error", e.getMessage());
    }
}
