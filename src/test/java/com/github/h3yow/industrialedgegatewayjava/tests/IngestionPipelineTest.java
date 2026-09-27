package com.github.h3yow.industrialedgegatewayjava.tests;

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
import com.github.h3yow.industrialedgegatewayjava.model.IngestResult;
import com.github.h3yow.industrialedgegatewayjava.model.Quality;
import com.github.h3yow.industrialedgegatewayjava.model.RawReading;
import com.github.h3yow.industrialedgegatewayjava.model.TelemetryPoint;
import com.github.h3yow.industrialedgegatewayjava.model.RejectReason;
import com.github.h3yow.industrialedgegatewayjava.session.SessionManager;
import com.github.h3yow.industrialedgegatewayjava.state.DeviceStateStore;
import com.github.h3yow.industrialedgegatewayjava.state.PointAccumulator;
import com.github.h3yow.industrialedgegatewayjava.transform.TelemetryConverter;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端采集流水线：会话 → 转换 → 累计 → 投递，重点验证
 * 重复不二次累计、坏质量不污染统计、计数对账可对上。
 */
class IngestionPipelineTest {

    private static final Logger log = LoggerFactory.getLogger(IngestionPipelineTest.class);

    record Harness(IngestionService ingestion, GatewayMetrics metrics, InMemoryTelemetrySink sink,
                   BackpressureDispatcher dispatcher, DeviceStateStore store, RawArchive archive,
                   WriteAheadJournal journal) {
    }

    private Harness harness(Path dataDir) {
        GatewayMetrics metrics = new GatewayMetrics();
        MappingRegistry registry = new MappingRegistry(MappingVersions.V1,
                MappingVersions.v1(), MappingVersions.v2());
        TelemetryConverter converter = new TelemetryConverter(registry, new PointMapper());
        DeviceStateStore store = new DeviceStateStore(dataDir == null ? null : dataDir.resolve("state.tsv"));
        SessionManager sessions = new SessionManager(store, 100);
        RawArchive archive = new RawArchive(1000);
        InMemoryTelemetrySink sink = new InMemoryTelemetrySink();
        WriteAheadJournal journal = new WriteAheadJournal(
                dataDir == null ? null : dataDir.resolve("wal.log"), new ObjectMapper(), 1000);
        DeadLetterLogger dl = new DeadLetterLogger(null, new ObjectMapper(), 100);
        BackpressureDispatcher dispatcher = new BackpressureDispatcher(
                sink, new RateLimiter(1_000_000, 1_000_000), journal, dl, metrics, 1000, 3, 5);
        dispatcher.start();
        IngestionService ingestion = new IngestionService(sessions, converter, store, archive,
                dispatcher, metrics, false);
        sessions.connect("device-A");
        return new Harness(ingestion, metrics, sink, dispatcher, store, archive, journal);
    }

    private RawReading reading(long seq, double value, Quality quality) {
        return new RawReading("device-A", seq, "temp_c", value, "C", quality, 1000L + seq);
    }

    @Test
    void duplicatesNotDoubleCountedAndBadQualityExcludedFromAccumulator() throws Exception {
        Harness h = harness(null);
        try {
            IngestResult r1 = h.ingestion.ingest(reading(1, 10.0, Quality.GOOD));
            IngestResult r2 = h.ingestion.ingest(reading(2, 20.0, Quality.GOOD));
            IngestResult bad = h.ingestion.ingest(reading(3, 999.0, Quality.BAD));
            IngestResult dup = h.ingestion.ingest(reading(2, 20.0, Quality.GOOD));

            log.info("[输入] seq=1 GOOD 判定={} 投递={}", r1.decision(), r1.dispatch().outcome());
            log.info("[输入] seq=2 GOOD 判定={} 投递={}", r2.decision(), r2.dispatch().outcome());
            log.info("[输入] seq=3 BAD  判定={} 异常标记={} 投递={}",
                    bad.decision(), bad.telemetry().attributes().get("abnormal"), bad.dispatch().outcome());
            log.info("[输入] seq=2 重发 判定={} 依据: 序列号水位已推进", dup.decision());

            assertEquals(RejectReason.ACCEPTED, r1.decision());
            assertEquals(RejectReason.BAD_QUALITY, bad.decision());
            assertEquals(RejectReason.DUPLICATE, dup.decision());
            assertEquals("true", bad.telemetry().attributes().get("abnormal"));

            h.dispatcher.awaitDrained(2000);

            PointAccumulator acc = h.store.accumulatorsOf("device-A").get("temperature");
            assertNotNull(acc);
            log.info("[状态] temperature 累计: count={} sum={} avg={} min={} max={} lastSeq={}",
                    acc.count(), acc.sum(), acc.average(), acc.min(), acc.max(), acc.lastSequence());
            assertEquals(2, acc.count(), "只有 2 条 GOOD 读数可累计，重复与坏质量都不得计入");
            assertEquals(30.0, acc.sum(), 1e-9);
            assertEquals(15.0, acc.average(), 1e-9);
            assertEquals(2, acc.lastSequence());
            assertEquals(3, h.store.lastContiguous("device-A"), "水位包含坏质量读数（连续性不依赖质量）");

            // 对账：received=4，accepted=2，badQuality=1，duplicate=1
            log.info("[对账] received={} accepted={} badQuality={} duplicate={} delivered={}",
                    h.metrics.received(),
                    h.metrics.decisionCount(RejectReason.ACCEPTED),
                    h.metrics.decisionCount(RejectReason.BAD_QUALITY),
                    h.metrics.decisionCount(RejectReason.DUPLICATE),
                    h.metrics.dispatchCount(com.github.h3yow.industrialedgegatewayjava.model.DispatchOutcome.DELIVERED));
            assertEquals(4, h.metrics.received());
            assertEquals(2, h.metrics.decisionCount(RejectReason.ACCEPTED));
            assertEquals(1, h.metrics.decisionCount(RejectReason.BAD_QUALITY));
            assertEquals(1, h.metrics.decisionCount(RejectReason.DUPLICATE));
            // 3 条遥测投递（含 1 条异常遥测），下游 distinct 也为 3
            assertEquals(3, h.sink.deliveredDistinctCount());
        } finally {
            h.dispatcher.stop();
        }
    }

    @Test
    void persistedStateSurvivesRestartAndReplaysDuplicates() throws Exception {
        Path dir = Path.of("target/gateway-test-data/restart-" + System.nanoTime());
        long watermark;
        Harness first = harness(dir);
        try {
            first.ingestion.ingest(reading(1, 10.0, Quality.GOOD));
            first.ingestion.ingest(reading(2, 20.0, Quality.GOOD));
            first.dispatcher.awaitDrained(2000);
            first.store.flush();
            watermark = first.store.lastContiguous("device-A");
            log.info("[重启前] 水位={} count={}", watermark,
                    first.store.accumulatorsOf("device-A").get("temperature").count());
        } finally {
            first.dispatcher.stop();
        }

        // 模拟进程重启：状态从磁盘加载，WAL 全部已 COMMIT
        Harness second = harness(dir);
        try {
            IngestResult replay = second.ingestion.ingest(reading(2, 20.0, Quality.GOOD));
            log.info("[重启后] 重放 seq=2 判定={} 依据: 持久化水位={}",
                    replay.decision(), second.store.lastContiguous("device-A"));
            assertEquals(RejectReason.DUPLICATE, replay.decision(), "重启后重放必须仍被识别为重复");
            PointAccumulator acc = second.store.accumulatorsOf("device-A").get("temperature");
            assertEquals(2, acc.count(), "累计状态恢复，重复数据未二次累计");
            assertEquals(30.0, acc.sum(), 1e-9);
            assertEquals(0, second.journal.recoveredPending().size(), "已确认的投递不应重放");
        } finally {
            second.dispatcher.stop();
        }
    }

    @Test
    void uncommittedWalRecordsAreRedeliveredAfterRestartExactlyOnce() {
        Path dir = Path.of("target/gateway-test-data/wal-restart-" + System.nanoTime());

        // 第一次：下游不可用，消息入队并写 WAL(PENDING)，但未 COMMIT
        WriteAheadJournal journal1 = new WriteAheadJournal(
                dir.resolve("wal2.log"), new ObjectMapper(), 1000);
        InMemoryTelemetrySink sink1 = new InMemoryTelemetrySink();
        sink1.setAvailable(false);
        GatewayMetrics metrics1 = new GatewayMetrics();
        BackpressureDispatcher d1 = new BackpressureDispatcher(
                sink1, new RateLimiter(1_000_000, 1_000_000), journal1,
                new DeadLetterLogger(null, new ObjectMapper(), 100), metrics1, 100, 1, 1);
        d1.start();
        RawReading raw = reading(5, 42.0, Quality.GOOD);
        TelemetryPoint tel = new TelemetryConverter(
                new MappingRegistry(MappingVersions.V1, MappingVersions.v1(), MappingVersions.v2()),
                new PointMapper()).convert(raw, 3000L, MappingVersions.V1).orElseThrow();
        d1.offer(tel);
        log.info("[崩溃前] tel seq=5 已写 WAL PENDING（下游不可用），未确认记录数={}",
                journal1.uncommittedCount());
        d1.stop(); // 模拟进程在投递确认前退出
        assertEquals(1, journal1.uncommittedCount());

        // 重启：新 journal 从磁盘恢复未确认记录并交给新的、已恢复可用的下游
        InMemoryTelemetrySink sink2 = new InMemoryTelemetrySink();
        WriteAheadJournal journal2 = new WriteAheadJournal(
                dir.resolve("wal2.log"), new ObjectMapper(), 1000);
        assertEquals(1, journal2.recoveredPending().size(), "重启必须恢复出 1 条未确认投递");
        log.info("[重启后] WAL 恢复未确认遥测 id={}", journal2.recoveredPending().get(0).telemetryId());
        GatewayMetrics metrics2 = new GatewayMetrics();
        BackpressureDispatcher d2 = new BackpressureDispatcher(
                sink2, new RateLimiter(1_000_000, 1_000_000), journal2,
                new DeadLetterLogger(null, new ObjectMapper(), 100), metrics2, 100, 1, 1);
        d2.start();
        try {
            d2.awaitDrained(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            d2.stop();
        }
        log.info("[对账] 恢复重放后下游 distinct={} DELIVERED={} 未确认残留={}",
                sink2.deliveredDistinctCount(),
                metrics2.dispatchCount(com.github.h3yow.industrialedgegatewayjava.model.DispatchOutcome.DELIVERED),
                journal2.uncommittedCount());
        assertEquals(1, sink2.deliveredDistinctCount(), "崩溃未丢数据，且重放不产生重复");
        assertEquals(1, metrics2.dispatchCount(
                com.github.h3yow.industrialedgegatewayjava.model.DispatchOutcome.DELIVERED));
        assertEquals(0, journal2.uncommittedCount(), "确认后必须 COMMIT 并清除在途状态");
    }
}
