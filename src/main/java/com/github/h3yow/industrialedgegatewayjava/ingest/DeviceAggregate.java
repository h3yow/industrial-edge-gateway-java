package com.github.h3yow.industrialedgegatewayjava.ingest;

import com.github.h3yow.industrialedgegatewayjava.model.DeviceSnapshot;
import com.github.h3yow.industrialedgegatewayjava.model.RawReading;
import com.github.h3yow.industrialedgegatewayjava.model.TelemetryEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 单台设备的序列号状态机（非线程安全，由 IngestionService 按设备串行调用）。
 *
 * <p>处理规则（每台设备独立，记 firstMissing = 水位+1 为最早缺失序号）：
 * <ol>
 *   <li><b>重复</b>：序列号已应用（在 seen 集合内）或已在乱序缓冲中 => 不重复累计；</li>
 *   <li><b>乱序-迟到</b>：序列号 &lt; firstMissing（落后于缺口起点且无法参与补齐）
 *       => 拒绝，不累计；</li>
 *   <li><b>乱序-超前（缺口）</b>：firstMissing &le; 序列号 且缺口跨度不超过缓冲容量
 *       => 进入 pending 缓冲，等待缺失序号补齐后按序应用；跨度超限则拒绝（GAP_SEQUENCE）；</li>
 *   <li><b>连续</b>：序列号 == firstMissing => 立即应用，并连带应用 pending 中连续的后续序号。</li>
 * </ol>
 *
 * <p>累计型指标在“应用”那一刻按标准值累加，每个序列号恰好一次；
 * 最新读数按指标名覆盖。因此重复/乱序不会造成重复累计。
 */
public class DeviceAggregate {

    /** 序列号从 0 开始；-1 表示尚无任何读数。 */
    static final long NO_WATERMARK = -1L;

    private final String deviceId;
    private final int gapBufferCapacity;
    private final Map<Long, Buffered> pending = new TreeMap<>();
    private final java.util.Set<Long> seen;
    private boolean connected;
    private long highWatermark = NO_WATERMARK;
    private final Map<String, Double> latestByMetric = new LinkedHashMap<>();
    private final Map<String, Double> totals = new LinkedHashMap<>();

    private record Buffered(TelemetryEvent event, boolean cumulative) {
    }

    /** 判定结果。 */
    public enum Decision { APPLIED, DUPLICATE, OUT_OF_ORDER, GAP_BUFFERED, GAP_REJECTED }

    public record AcceptResult(Decision decision, List<TelemetryEvent> appliedEvents) {
    }

    public DeviceAggregate(String deviceId) {
        this(deviceId, 64, 512);
    }

    public DeviceAggregate(String deviceId, int gapBufferCapacity, int seenRetention) {
        this.deviceId = deviceId;
        this.gapBufferCapacity = gapBufferCapacity;
        // 有界 LRU（本类由 IngestionService 的设备锁串行访问，无需内部同步）：
        // 旧序列号淘汰后若重放，会被判为 OUT_OF_ORDER 而非 DUPLICATE，两种都不会重复累计。
        this.seen = java.util.Collections.newSetFromMap(
                new java.util.LinkedHashMap<>(seenRetention, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<Long, Boolean> eldest) {
                        return size() > seenRetention;
                    }
                });
    }

    public String deviceId() {
        return deviceId;
    }

    public void connected(boolean value) {
        this.connected = value;
    }

    public boolean connected() {
        return connected;
    }

    public long highWatermark() {
        return highWatermark;
    }

    public int pendingCount() {
        return pending.size();
    }

    /**
     * 接收一条已通过质量/时间/映射校验的读数。
     *
     * @return 判定结果；appliedEvents 为本次新达到可投递状态的事件（可能连带多条 pending）
     */
    public AcceptResult accept(RawReading reading, TelemetryEvent event, boolean cumulative) {
        long seq = reading.sequence();
        long firstMissing = highWatermark + 1;

        if (seen.contains(seq) || pending.containsKey(seq)) {
            return new AcceptResult(Decision.DUPLICATE, List.of());
        }
        if (seq < firstMissing) {
            // 落后于缺口起点：迟到且无法补序的乱序数据。
            return new AcceptResult(Decision.OUT_OF_ORDER, List.of());
        }
        if (seq > firstMissing) {
            // 缓冲同时受“跨度上限”和“已占用条数上限”约束。
            long span = seq - firstMissing + 1;
            if (span > gapBufferCapacity || pending.size() >= gapBufferCapacity) {
                return new AcceptResult(Decision.GAP_REJECTED, List.of());
            }
            pending.put(seq, new Buffered(event, cumulative));
            return new AcceptResult(Decision.GAP_BUFFERED, List.of());
        }

        // seq == firstMissing：连续到达，应用并连带排空缓冲。
        List<TelemetryEvent> applied = new ArrayList<>();
        apply(event, cumulative);
        applied.add(event);
        long next = highWatermark + 1;
        while (true) {
            Buffered b = pending.remove(next);
            if (b == null) {
                break;
            }
            apply(b.event(), b.cumulative());
            applied.add(b.event());
        }
        return new AcceptResult(Decision.APPLIED, applied);
    }

    private void apply(TelemetryEvent event, boolean cumulative) {
        highWatermark = event.sequence();
        seen.add(event.sequence());
        latestByMetric.put(event.metricName(), event.value());
        if (cumulative) {
            totals.merge(event.metricName(), event.value(), Double::sum);
        }
    }

    public DeviceSnapshot snapshot() {
        return new DeviceSnapshot(deviceId, connected, highWatermark,
                Map.copyOf(latestByMetric), Map.copyOf(totals));
    }
}
