package com.github.h3yow.industrialedgegatewayjava.downstream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.industrialedgegatewayjava.model.TelemetryPoint;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 投递预写日志（WAL），保证下游投递的“重启不丢、可重放”：
 * <ul>
 *   <li>入队先写 {@code P <id> <json>}（PENDING），投递确认后写 {@code C <id>}（COMMIT）；</li>
 *   <li>进程重启后，所有有 P 无 C 的遥测会被重新投递（下游按 telemetryId 幂等去重）；</li>
 *   <li>日志有容量上限；达到上限时 {@link #recordPending} 返回 false，调用方据此拒绝（不会无限缓存）。</li>
 * </ul>
 * 文件不存在或传入 null 时退化为内存模式（仅跟踪未确认 id，不落盘），供测试使用。
 */
public class WriteAheadJournal {

    private final Path file;
    private final ObjectMapper objectMapper;
    private final long maxPendingRecords;
    private final Set<String> uncommitted = new LinkedHashSet<>();
    private Writer appender;
    private int commitsSinceCompaction = 0;
    private int compactEveryCommits = 200;

    public WriteAheadJournal(Path file, ObjectMapper objectMapper, long maxPendingRecords) {
        this.file = file;
        this.objectMapper = objectMapper;
        this.maxPendingRecords = maxPendingRecords;
        if (file != null && Files.exists(file)) {
            recover();
        }
        if (file != null) {
            try {
                if (file.getParent() != null) {
                    Files.createDirectories(file.getParent());
                }
                this.appender = new OutputStreamWriter(
                        Files.newOutputStream(file, StandardOpenOption.CREATE, StandardOpenOption.APPEND),
                        StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * 记录一条待投递遥测。
     *
     * @return false 表示未确认记录已达上限，调用方应拒绝本次入队
     */
    public synchronized boolean recordPending(TelemetryPoint t) {
        if (uncommitted.contains(t.telemetryId())) {
            return true; // 已在途（重复入队请求），视为成功登记
        }
        if (uncommitted.size() >= maxPendingRecords) {
            return false;
        }
        uncommitted.add(t.telemetryId());
        if (file != null) {
            try {
                appender.write("P\t" + t.telemetryId() + "\t"
                        + objectMapper.writeValueAsString(t) + "\n");
                appender.flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return true;
    }

    public synchronized void recordCommit(String telemetryId) {
        if (uncommitted.remove(telemetryId) && file != null) {
            try {
                appender.write("C\t" + telemetryId + "\n");
                appender.flush();
                if (++commitsSinceCompaction >= compactEveryCommits) {
                    compact();
                    commitsSinceCompaction = 0;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * 压缩日志：只保留仍未确认（有 P 无 C/X）的记录，重写到新文件后原子替换。
     */
    private void compact() throws IOException {
        List<String> pendingLines = new ArrayList<>();
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) {
                int tab1 = line.indexOf('\t');
                if (tab1 > 0 && "P".equals(line.substring(0, tab1))) {
                    int tab2 = line.indexOf('\t', tab1 + 1);
                    String id = line.substring(tab1 + 1, tab2);
                    if (uncommitted.contains(id)) {
                        pendingLines.add(line);
                    }
                }
            }
        }
        Path compacted = file.resolveSibling(file.getFileName() + ".compact");
        try (Writer w = new OutputStreamWriter(Files.newOutputStream(compacted), StandardCharsets.UTF_8)) {
            for (String l : pendingLines) {
                w.write(l);
                w.write('\n');
            }
        }
        appender.close();
        Files.move(compacted, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        appender = new OutputStreamWriter(
                Files.newOutputStream(file, StandardOpenOption.CREATE, StandardOpenOption.APPEND),
                StandardCharsets.UTF_8);
    }

    /**
     * 撤销在途登记（有界队列入队失败时使用）。磁盘上追加 X 回滚标记，与 COMMIT 一样会压缩清理。
     */
    public synchronized void rollbackPending(String telemetryId) {
        if (uncommitted.remove(telemetryId) && file != null) {
            try {
                appender.write("X\t" + telemetryId + "\n");
                appender.flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * 重启恢复：读取日志，返回有 P 无 C 的遥测（按出现顺序、去重）。
     */
    private void recover() {
        List<TelemetryPoint> pending = new ArrayList<>();
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                int tab1 = line.indexOf('\t');
                if (tab1 < 0) {
                    continue;
                }
                String kind = line.substring(0, tab1);
                if ("C".equals(kind) || "X".equals(kind)) {
                    uncommitted.remove(line.substring(tab1 + 1));
                } else if ("P".equals(kind)) {
                    int tab2 = line.indexOf('\t', tab1 + 1);
                    String id = line.substring(tab1 + 1, tab2);
                    String json = line.substring(tab2 + 1);
                    uncommitted.add(id);
                    pending.add(objectMapper.readValue(json, TelemetryPoint.class));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        recovered = pending.stream().filter(t -> uncommitted.contains(t.telemetryId())).toList();
    }

    private List<TelemetryPoint> recovered = List.of();

    public synchronized List<TelemetryPoint> recoveredPending() {
        return recovered;
    }

    public synchronized int uncommittedCount() {
        return uncommitted.size();
    }

    public synchronized Set<String> uncommittedIds() {
        return Set.copyOf(uncommitted);
    }
}
