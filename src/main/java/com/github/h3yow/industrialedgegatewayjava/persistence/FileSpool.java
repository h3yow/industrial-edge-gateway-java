package com.github.h3yow.industrialedgegatewayjava.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.industrialedgegatewayjava.model.TelemetryEvent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 基于本地 JSON-Lines 文件的有界缓存（WAL 语义）。
 *
 * <p>每条 offer 以一行 JSON 原子追加；poll 取出后用临时文件重写剩余行。
 * 构造时从文件恢复未确认事件，因此进程重启不会悄悄丢失已采集数据。
 * 不依赖任何外部服务或凭据。损坏行会被跳过并记录到独立 .dead-letter 文件，
 * 不参与后续处理（可人工核查），避免一条坏行阻塞全部数据。
 */
public class FileSpool implements Spool {

    private final int capacity;
    private final Path file;
    private final Path deadLetter;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Object lock = new Object();
    private final List<TelemetryEvent> queue = new ArrayList<>();

    public FileSpool(Path directory, int capacity) {
        this.capacity = capacity;
        try {
            Files.createDirectories(directory);
            this.file = directory.resolve("spool.log");
            this.deadLetter = directory.resolve("spool.dead-letter.log");
            if (Files.exists(file)) {
                recover();
            } else {
                Files.createFile(file);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot init file spool in " + directory, e);
        }
    }

    private void recover() throws IOException {
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        List<String> valid = new ArrayList<>(lines.size());
        for (String line : lines) {
            if (line.isBlank()) {
                continue;
            }
            try {
                queue.add(mapper.readValue(line, TelemetryEvent.class));
                valid.add(line);
            } catch (IOException parseFailure) {
                Files.writeString(deadLetter, line + System.lineSeparator(), StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
        }
        rewrite();
    }

    @Override
    public boolean offer(TelemetryEvent event) {
        synchronized (lock) {
            if (queue.size() >= capacity) {
                return false;
            }
            try {
                String line = mapper.writeValueAsString(event);
                Files.writeString(file, line + System.lineSeparator(), StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                queue.add(event);
                return true;
            } catch (IOException e) {
                throw new UncheckedIOException("spool append failed", e);
            }
        }
    }

    @Override
    public void prepend(List<TelemetryEvent> events) {
        synchronized (lock) {
            queue.addAll(0, new ArrayList<>(events));
            rewrite();
        }
    }

    @Override
    public List<TelemetryEvent> poll(int max) {
        synchronized (lock) {
            List<TelemetryEvent> out = new ArrayList<>(Math.min(max, queue.size()));
            while (out.size() < max && !queue.isEmpty()) {
                out.add(queue.remove(0));
            }
            if (!out.isEmpty()) {
                rewrite();
            }
            return out;
        }
    }

    private void rewrite() {
        try {
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            StringBuilder sb = new StringBuilder();
            for (TelemetryEvent event : queue) {
                sb.append(mapper.writeValueAsString(event)).append(System.lineSeparator());
            }
            Files.writeString(tmp, sb.toString(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("spool rewrite failed", e);
        }
    }

    @Override
    public int size() {
        synchronized (lock) {
            return queue.size();
        }
    }

    @Override
    public int capacity() {
        return capacity;
    }
}
