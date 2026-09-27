package com.github.h3yow.industrialedgegatewayjava.ingest;

import com.github.h3yow.industrialedgegatewayjava.archive.RawArchive;
import com.github.h3yow.industrialedgegatewayjava.downstream.BackpressureDispatcher;
import com.github.h3yow.industrialedgegatewayjava.metrics.GatewayMetrics;
import com.github.h3yow.industrialedgegatewayjava.model.DispatchResult;
import com.github.h3yow.industrialedgegatewayjava.model.GapAnomaly;
import com.github.h3yow.industrialedgegatewayjava.model.IngestResult;
import com.github.h3yow.industrialedgegatewayjava.model.Quality;
import com.github.h3yow.industrialedgegatewayjava.model.RawReading;
import com.github.h3yow.industrialedgegatewayjava.model.RejectReason;
import com.github.h3yow.industrialedgegatewayjava.model.SessionOutcome;
import com.github.h3yow.industrialedgegatewayjava.model.TelemetryPoint;
import com.github.h3yow.industrialedgegatewayjava.session.SessionManager;
import com.github.h3yow.industrialedgegatewayjava.state.DeviceStateStore;
import com.github.h3yow.industrialedgegatewayjava.transform.TelemetryConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 采集编排服务：会话去重 → 质量判定 → 单位/点位转换 → 累计状态 → 下游背压投递。
 *
 * 对账语义：
 * <ul>
 *   <li>DUPLICATE / STALE 原样返回，不转换、不累计、不投递；</li>
 *   <li>BAD_QUALITY：转换为带 abnormal 标记的遥测并投递，但不写入累计器（不当正常数据吞掉）；</li>
 *   <li>未映射点位：计入 unmapped，原始读数仍归档；</li>
 *   <li>每次按序释放都同步抬高持久化的序列号水位，重启后重复上报仍被拒绝。</li>
 * </ul>
 */
public class IngestionService {

    private final SessionManager sessions;
    private final TelemetryConverter converter;
    private final DeviceStateStore stateStore;
    private final RawArchive archive;
    private final BackpressureDispatcher dispatcher;
    private final GatewayMetrics metrics;
    private final boolean flushStatePerRead;

    public IngestionService(SessionManager sessions, TelemetryConverter converter, DeviceStateStore stateStore,
                            RawArchive archive, BackpressureDispatcher dispatcher, GatewayMetrics metrics,
                            boolean flushStatePerRead) {
        this.sessions = sessions;
        this.converter = converter;
        this.stateStore = stateStore;
        this.archive = archive;
        this.dispatcher = dispatcher;
        this.metrics = metrics;
        this.flushStatePerRead = flushStatePerRead;
    }

    /**
     * 处理一次设备上报，返回该读数（incoming）的最终结论；
     * 若本次上报连带把 pending 中的读数按序释放，其余结果通过 {@link #ingestBatch} 获得。
     */
    public IngestResult ingest(RawReading reading) {
        return ingestBatch(reading).get(0);
    }

    /**
     * 与 {@link #ingest(RawReading)} 相同，但返回本次上报连带按序释放的全部读数结论。
     */
    public List<IngestResult> ingestBatch(RawReading reading) {
        metrics.recordReceived();
        SessionOutcome outcome = sessions.session(reading.deviceId()).handle(reading);

        if (outcome.decision() == RejectReason.DUPLICATE || outcome.decision() == RejectReason.STALE) {
            metrics.recordDecision(outcome.decision());
            return List.of(new IngestResult(reading, outcome.decision(), null, null, List.of(), List.of()));
        }

        for (GapAnomaly g : outcome.anomalies()) {
            metrics.recordGap();
        }

        List<IngestResult> results = new ArrayList<>();
        boolean incomingReleased = false;
        for (RawReading released : outcome.released()) {
            incomingReleased |= released == reading;
            results.add(processReleased(released, outcome.anomalies(), released == reading));
        }
        if (!incomingReleased) {
            // 读数已被会话层接受，但仍在乱序缓存中等待空洞填补：明确返回“待释放”，不转换不投递
            results.add(new IngestResult(reading, RejectReason.ACCEPTED, null, null,
                    outcome.anomalies(), List.of()));
        }
        if (flushStatePerRead) {
            stateStore.flush();
        }
        return results;
    }

    private IngestResult processReleased(RawReading reading, List<GapAnomaly> anomalies, boolean isIncoming) {
        long ingestTime = System.currentTimeMillis();
        archive.append(reading);
        stateStore.advanceSequence(reading.deviceId(), reading.sequence());

        RejectReason effectiveDecision = RejectReason.ACCEPTED;
        if (reading.quality() != Quality.GOOD) {
            effectiveDecision = RejectReason.BAD_QUALITY;
        }

        Optional<TelemetryPoint> mapped = converter.convert(reading, ingestTime);
        TelemetryPoint telemetry = null;
        DispatchResult dispatch = null;

        if (mapped.isEmpty()) {
            metrics.recordUnmapped();
        } else {
            telemetry = mapped.get();
            if (reading.quality() == Quality.GOOD) {
                // 只有 GOOD 读数参与累计；异常值进入异常遥测但不污染统计
                stateStore.accumulate(reading.deviceId(), telemetry.metricName(),
                        telemetry.value(), telemetry.eventTimeMs(), reading.sequence());
            }
            dispatch = dispatcher.offer(telemetry);
        }

        metrics.recordDecision(effectiveDecision);
        return new IngestResult(reading, effectiveDecision, telemetry, dispatch,
                isIncoming ? anomalies : List.of(), List.of());
    }
}
