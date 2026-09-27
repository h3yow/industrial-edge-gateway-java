package com.github.h3yow.industrialedgegatewayjava.state;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 设备累计状态存储。
 *
 * 持久化两类信息（原子快照文件）：
 * <ol>
 *   <li>每台设备已确认连续的序列号水位 —— 重启后据此拒绝重复上报，避免重复累计；</li>
 *   <li>每个“设备+指标”的累计器（次数/和/极值/最新值）—— 重启后累计结果连续可解释。</li>
 * </ol>
 * 传入 {@code null} 路径时退化为纯内存存储（供单元测试与无盘环境使用）。
 */
public class DeviceStateStore {

    private final Path file;
    private final Map<String, Long> sequences = new HashMap<>();
    private final Map<String, Map<String, PointAccumulator>> accumulators = new HashMap<>();

    public DeviceStateStore(Path file) {
        this.file = file;
        if (file != null) {
            load();
        }
    }

    public synchronized long lastContiguous(String deviceId) {
        return sequences.getOrDefault(deviceId, 0L);
    }

    /**
     * 抬高设备序列号水位（只增不减）。
     */
    public synchronized void advanceSequence(String deviceId, long newContiguous) {
        long old = sequences.getOrDefault(deviceId, 0L);
        if (newContiguous > old) {
            sequences.put(deviceId, newContiguous);
        }
    }

    /**
     * 对累计器应用一次有效读数（同一序列号在调用方已被去重，因此这里天然不会重复累计）。
     */
    public synchronized void accumulate(String deviceId, String metric,
                                        double value, long eventTimeMs, long sequence) {
        accumulators.computeIfAbsent(deviceId, k -> new LinkedHashMap<>())
                .computeIfAbsent(metric, k -> new PointAccumulator())
                .accept(value, eventTimeMs, sequence);
    }

    public synchronized Map<String, PointAccumulator> accumulatorsOf(String deviceId) {
        return Map.copyOf(accumulators.getOrDefault(deviceId, Map.of()));
    }

    /** 全部设备的累计器快照（REST 诊断用）。 */
    public synchronized Map<String, Map<String, PointAccumulator>> snapshot() {
        Map<String, Map<String, PointAccumulator>> copy = new LinkedHashMap<>();
        accumulators.forEach((d, m) -> copy.put(d, new LinkedHashMap<>(m)));
        return copy;
    }

    public synchronized Map<String, Long> sequenceSnapshot() {
        return Map.copyOf(sequences);
    }

    /**
     * 原子写入快照（tmp + move）。
     */
    public synchronized void flush() {
        if (file == null) {
            return;
        }
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        try {
            if (file.getParent() != null) {
                Files.createDirectories(file.getParent());
            }
            try (Writer w = new OutputStreamWriter(Files.newOutputStream(tmp), StandardCharsets.UTF_8)) {
                for (Map.Entry<String, Long> e : sequences.entrySet()) {
                    w.write("SEQ\t" + e.getKey() + "\t" + e.getValue() + "\n");
                }
                for (Map.Entry<String, Map<String, PointAccumulator>> dev : accumulators.entrySet()) {
                    for (Map.Entry<String, PointAccumulator> pt : dev.getValue().entrySet()) {
                        PointAccumulator a = pt.getValue();
                        w.write("ACC\t" + dev.getKey() + "\t" + pt.getKey() + "\t"
                                + a.count() + "\t" + a.sum() + "\t" + a.min() + "\t" + a.max() + "\t"
                                + (a.lastValue() == null ? "" : a.lastValue()) + "\t"
                                + a.lastEventTimeMs() + "\t" + a.lastSequence() + "\n");
                    }
                }
            }
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new IllegalStateException("状态快照写入失败: " + file, e);
        }
    }

    private void load() {
        if (!Files.exists(file)) {
            return;
        }
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                String[] f = line.split("\t", -1);
                if ("SEQ".equals(f[0]) && f.length == 3) {
                    sequences.put(f[1], Long.parseLong(f[2]));
                } else if ("ACC".equals(f[0]) && f.length == 10) {
                    String device = f[1];
                    String metric = f[2];
                    long count = Long.parseLong(f[3]);
                    double sum = Double.parseDouble(f[4]);
                    double min = Double.parseDouble(f[5]);
                    double max = Double.parseDouble(f[6]);
                    Double lastValue = f[7].isEmpty() ? null : Double.parseDouble(f[7]);
                    long eventTime = Long.parseLong(f[8]);
                    long lastSeq = Long.parseLong(f[9]);
                    accumulators.computeIfAbsent(device, k -> new LinkedHashMap<>())
                            .put(metric, PointAccumulator.restore(count, sum, min, max, lastValue, eventTime, lastSeq));
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("状态快照读取失败: " + file, e);
        }
    }
}
