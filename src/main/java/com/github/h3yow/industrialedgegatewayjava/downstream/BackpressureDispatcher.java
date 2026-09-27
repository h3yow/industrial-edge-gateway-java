package com.github.h3yow.industrialedgegatewayjava.downstream;

import com.github.h3yow.industrialedgegatewayjava.metrics.GatewayMetrics;
import com.github.h3yow.industrialedgegatewayjava.model.DispatchOutcome;
import com.github.h3yow.industrialedgegatewayjava.model.DispatchResult;
import com.github.h3yow.industrialedgegatewayjava.model.TelemetryPoint;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 带背压的下游投递器，策略有明确区分与上限：
 * <ol>
 *   <li><b>限流 RATE_LIMITED</b>：令牌桶拿不到令牌（依据：可用令牌为 0），消息不进队列，立即拒绝；</li>
 *   <li><b>缓存 BUFFER_FULL</b>：有界队列（容量 {@code bufferCapacity}）已满，或 WAL 未确认记录达上限，立即拒绝；</li>
 *   <li><b>缓存后投递 QUEUED</b>：入队即写 WAL(PENDING)，由单线程 worker 顺序投递；</li>
 *   <li><b>重试与死信 DEAD_LETTER</b>：下游不可用/异常时按次数上限重试；耗尽后记死信并 WAL 保留，绝不悄悄丢失；</li>
 *   <li>投递成功后写 WAL(COMMIT)。重启时重放未确认记录，下游按 telemetryId 幂等，重复可辨认。</li>
 * </ol>
 */
public class BackpressureDispatcher {

    private final DownstreamSink sink;
    private final RateLimiter rateLimiter;
    private final WriteAheadJournal journal;
    private final DeadLetterLogger deadLetterLogger;
    private final GatewayMetrics metrics;
    private final int bufferCapacity;
    private final int maxAttempts;
    private final long retryBackoffMs;

    private final LinkedBlockingQueue<TelemetryPoint> queue;
    private final AtomicInteger queued = new AtomicInteger();
    private volatile boolean running = true;
    private Thread worker;

    public BackpressureDispatcher(DownstreamSink sink, RateLimiter rateLimiter, WriteAheadJournal journal,
                                  DeadLetterLogger deadLetterLogger, GatewayMetrics metrics,
                                  int bufferCapacity, int maxAttempts, long retryBackoffMs) {
        this.sink = sink;
        this.rateLimiter = rateLimiter;
        this.journal = journal;
        this.deadLetterLogger = deadLetterLogger;
        this.metrics = metrics;
        this.bufferCapacity = bufferCapacity;
        this.maxAttempts = maxAttempts;
        this.retryBackoffMs = retryBackoffMs;
        this.queue = new LinkedBlockingQueue<>(bufferCapacity);
    }

    /**
     * 启动后台投递线程，并重放 WAL 中未确认的记录。
     */
    public synchronized void start() {
        for (TelemetryPoint t : journal.recoveredPending()) {
            if (queue.offer(t)) {
                queued.incrementAndGet();
            }
        }
        worker = new Thread(this::runLoop, "downstream-dispatcher");
        worker.setDaemon(true);
        worker.start();
    }

    public synchronized void stop() {
        running = false;
        if (worker != null) {
            worker.interrupt();
        }
    }

    /**
     * 非阻塞投递入口，立即给出可解释的判定结果。
     */
    public DispatchResult offer(TelemetryPoint t) {
        if (!rateLimiter.tryAcquire()) {
            DispatchResult r = new DispatchResult(DispatchOutcome.RATE_LIMITED, t.telemetryId(),
                    "限流：当前可用令牌=0（速率上限保护下游），消息未入队");
            metrics.recordDispatch(DispatchOutcome.RATE_LIMITED);
            return r;
        }
        // 先检查有界队列容量（队列是第一道明确上限），再尝试 WAL 登记（在途记录第二道上限）
        if (queue.size() >= bufferCapacity) {
            DispatchResult r = new DispatchResult(DispatchOutcome.BUFFER_FULL, t.telemetryId(),
                    "缓存拒绝：有界投递队列已满（容量=" + bufferCapacity + "），消息未入队");
            metrics.recordDispatch(DispatchOutcome.BUFFER_FULL);
            return r;
        }
        if (!journal.recordPending(t)) {
            DispatchResult r = new DispatchResult(DispatchOutcome.BUFFER_FULL, t.telemetryId(),
                    "缓存拒绝：WAL 未确认记录已达上限（在途数据保护上限），消息未入队");
            metrics.recordDispatch(DispatchOutcome.BUFFER_FULL);
            return r;
        }
        if (!queue.offer(t)) {
            // 极端竞争下入队失败：撤销刚登记的在途占用
            journal.rollbackPending(t.telemetryId());
            DispatchResult r = new DispatchResult(DispatchOutcome.BUFFER_FULL, t.telemetryId(),
                    "缓存拒绝：有界投递队列竞争已满（容量=" + bufferCapacity + "），消息未入队");
            metrics.recordDispatch(DispatchOutcome.BUFFER_FULL);
            return r;
        }
        queued.incrementAndGet();
        return new DispatchResult(DispatchOutcome.QUEUED, t.telemetryId(),
                "已入有界缓存等待异步投递，当前队列深度=" + queued.get());
    }

    public int queueDepth() {
        return queued.get();
    }

    /** 等待队列排空（仅测试/优雅停机使用）。 */
    public void awaitDrained(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (queued.get() > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
    }

    private void runLoop() {
        while (running) {
            try {
                TelemetryPoint t = queue.poll(500, TimeUnit.MILLISECONDS);
                if (t == null) {
                    continue;
                }
                deliverWithRetry(t);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                // worker 绝不能因单条异常静默死掉
            }
        }
    }

    private void deliverWithRetry(TelemetryPoint t) {
        int attempts = 0;
        try {
            while (attempts < maxAttempts) {
                attempts++;
                SinkResult result;
                try {
                    result = sink.deliver(t);
                } catch (RuntimeException e) {
                    result = SinkResult.fail("exception: " + e);
                }
                if (result.accepted()) {
                    journal.recordCommit(t.telemetryId());
                    if (result.duplicate()) {
                        // ACK 丢失导致的重投：下游幂等识别，按一次成功投递对账，不产生重复数据
                        metrics.recordDispatch(DispatchOutcome.DELIVERED);
                    } else {
                        metrics.recordDispatch(DispatchOutcome.DELIVERED);
                    }
                    return;
                }
                if (attempts < maxAttempts) {
                    Thread.sleep(retryBackoffMs);
                }
            }
            deadLetterLogger.record(t, "重试 " + maxAttempts + " 次仍失败，转入死信", attempts);
            metrics.recordDispatch(DispatchOutcome.DEAD_LETTER);
            // 死信保留在 WAL 中（不写 COMMIT），重启后仍可辨认与人工重放
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            queued.decrementAndGet();
        }
    }
}
