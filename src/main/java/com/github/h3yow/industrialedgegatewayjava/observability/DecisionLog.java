package com.github.h3yow.industrialedgegatewayjava.observability;

import com.github.h3yow.industrialedgegatewayjava.model.DecisionRecord;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

/**
 * 判定日志：记录输入、判定依据与关键状态变化。
 *
 * <p>内存留存（有界，防止演示/长测时无限增长），同时输出到 SLF4J。
 * 测试通过 {@link #dump(String)} 打印输入、判定依据与关键状态变化。
 */
public class DecisionLog {

    /** 单设备留存条数上限，超出后丢弃最旧记录（日志仅用于观测，不参与正确性）。 */
    static final int MAX_ENTRIES_PER_DEVICE = 500;

    private final ConcurrentLinkedDeque<DecisionRecord> entries = new ConcurrentLinkedDeque<>();

    public void log(String stage, String deviceId, long sequence, String decision, String reason, String detail) {
        DecisionRecord record = new DecisionRecord(stage, deviceId, sequence, decision, reason, detail);
        entries.addLast(record);
        trim(deviceId);
        org.slf4j.LoggerFactory.getLogger(DecisionLog.class)
                .info("[decision] stage={} device={} seq={} decision={} reason={} detail={}",
                        stage, deviceId, sequence, decision, reason, detail);
    }

    private void trim(String deviceId) {
        long count = entries.stream().filter(e -> e.deviceId().equals(deviceId)).count();
        while (count > MAX_ENTRIES_PER_DEVICE) {
            entries.pollFirst();
            count--;
        }
    }

    public List<DecisionRecord> entries() {
        return List.copyOf(entries);
    }

    public List<DecisionRecord> forDevice(String deviceId) {
        return entries.stream().filter(e -> e.deviceId().equals(deviceId)).toList();
    }

    /** 便于测试断言与阅读：打印某设备的全部判定链路。 */
    public String dump(String deviceId) {
        String text = forDevice(deviceId).stream()
                .map(e -> "  - [%s] seq=%d => %s%s %s"
                        .formatted(e.stage(), e.sequence(), e.decision(),
                                e.reason() == null ? "" : " reason=" + e.reason(),
                                e.detail() == null ? "" : e.detail()))
                .collect(Collectors.joining("\n"));
        String banner = "Decision trace for device " + deviceId + ":\n" + text;
        System.out.println(banner);
        return banner;
    }
}
