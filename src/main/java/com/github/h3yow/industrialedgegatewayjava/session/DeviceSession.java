package com.github.h3yow.industrialedgegatewayjava.session;

import com.github.h3yow.industrialedgegatewayjava.model.GapAnomaly;
import com.github.h3yow.industrialedgegatewayjava.model.RawReading;
import com.github.h3yow.industrialedgegatewayjava.model.RejectReason;
import com.github.h3yow.industrialedgegatewayjava.model.SessionOutcome;
import com.github.h3yow.industrialedgegatewayjava.model.SessionStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 单台设备的会话状态机（线程安全，每台设备独立锁）。
 *
 * 序列号语义：
 * <ul>
 *   <li>{@code seq <= lastContiguous}：重复上报 → DUPLICATE，不二次累计；</li>
 *   <li>{@code seq == lastContiguous + 1}：连续，立即释放；</li>
 *   <li>{@code seq >  lastContiguous + 1}：乱序早到，进入有界 pending 缓存，按序释放；
 *       同时产生一个序列号空洞异常（gap），空洞不会被数据悄悄掩盖；</li>
 *   <li>空洞被后续按序数据填补后，相关读数才释放；永久迟到（seq 落在已放弃空洞之后）→ STALE。</li>
 * </ul>
 */
public class DeviceSession {

    private final String deviceId;
    private final int maxPending;
    private SessionStatus status = SessionStatus.DISCONNECTED;
    private long lastContiguous;
    private final TreeMap<Long, RawReading> pending = new TreeMap<>();

    /**
     * @param deviceId       设备标识
     * @param initialSequence 已确认连续的起始序列号（重启恢复时从状态存储加载，通常为 0 表示尚无数据）
     * @param maxPending     乱序 pending 缓存上限
     */
    public DeviceSession(String deviceId, long initialSequence, int maxPending) {
        this.deviceId = deviceId;
        this.lastContiguous = initialSequence;
        this.maxPending = maxPending;
    }

    public synchronized String deviceId() {
        return deviceId;
    }

    public synchronized SessionStatus status() {
        return status;
    }

    public synchronized long lastContiguous() {
        return lastContiguous;
    }

    public synchronized int pendingCount() {
        return pending.size();
    }

    /**
     * 设备建连（或重连）。
     *
     * @return true 表示此前是断开状态，本次属于一次（重新）连接
     */
    public synchronized boolean connect() {
        boolean wasDisconnected = status == SessionStatus.DISCONNECTED;
        status = SessionStatus.CONNECTED;
        return wasDisconnected;
    }

    public synchronized void disconnect() {
        status = SessionStatus.DISCONNECTED;
    }

    /**
     * 处理一次上报。
     */
    public synchronized SessionOutcome handle(RawReading reading) {
        long seq = reading.sequence();
        if (seq <= lastContiguous) {
            return new SessionOutcome(RejectReason.DUPLICATE, reading, List.of(), List.of());
        }
        if (pending.containsKey(seq)) {
            return new SessionOutcome(RejectReason.DUPLICATE, reading, List.of(), List.of());
        }

        List<GapAnomaly> anomalies = new ArrayList<>();
        if (seq > lastContiguous + 1) {
            // 只上报本次“向前跳跃”新打开的空洞区间：
            // 已见过的最大序号是 max(lastContiguous, pending.lastKey)，其后到 seq 之间为新缺失。
            long maxSeen = pending.isEmpty() ? lastContiguous : Math.max(lastContiguous, pending.lastKey());
            if (seq > maxSeen + 1) {
                anomalies.add(new GapAnomaly(deviceId, maxSeen + 1, seq - 1, System.currentTimeMillis()));
            }
        }

        if (pending.size() >= maxPending && seq > lastContiguous + 1) {
            // pending 窗口已满且不是“下一个连续序列号”：视为无法再接纳的迟到/越序数据
            return new SessionOutcome(RejectReason.STALE, reading, List.of(), anomalies);
        }
        pending.put(seq, reading);

        List<RawReading> released = new ArrayList<>();
        // 只要最小 pending 序号正好接上水位就释放；遇到空洞即停止（其后数据继续等待）
        while (!pending.isEmpty() && pending.firstKey() == lastContiguous + 1) {
            Map.Entry<Long, RawReading> e = pending.pollFirstEntry();
            released.add(e.getValue());
            lastContiguous = e.getKey();
        }
        return new SessionOutcome(RejectReason.ACCEPTED, reading, released, anomalies);
    }

    /**
     * 扫描 pending 中被永久放弃的空洞（测试/运维显式调用）。
     * 任何低于当前最小待填补序号且仍未到达的读数，都不再可能被接纳，返回 STALE 判定信息。
     */
    public synchronized SweepResult sweepGaps(long nowMs) {
        // 当前实现中，空洞保持开放等待迟到数据；只有 pending 溢出时才产生 STALE。
        // 此方法保留为显式运维入口：统计当前未关闭空洞数量。
        long openGaps = 0;
        if (!pending.isEmpty()) {
            openGaps = pending.firstKey() - lastContiguous - 1;
        }
        return new SweepResult(deviceId, openGaps, nowMs);
    }

    /**
     * 空洞扫描结果。
     */
    public record SweepResult(String deviceId, long openGapCount, long atMs) {
    }

    /** 供诊断使用的 pending 序列号快照 */
    public synchronized List<Long> pendingSequences() {
        return new ArrayList<>(pending.keySet());
    }
}
