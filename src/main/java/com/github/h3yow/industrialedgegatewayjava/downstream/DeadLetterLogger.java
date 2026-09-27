package com.github.h3yow.industrialedgegatewayjava.downstream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.industrialedgegatewayjava.model.TelemetryPoint;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 死信记录：重试耗尽后遥测被标记 DEAD_LETTER 并持久化/保留，丢弃必须可辨认、可追溯，不允许悄悄丢失。
 * 传入 null 路径时仅保留在内存（有界）。
 */
public class DeadLetterLogger {

    private final Path file;
    private final ObjectMapper objectMapper;
    private final int memoryCapacity;
    private final List<DeadLetter> inMemory = new ArrayList<>();

    public record DeadLetter(TelemetryPoint telemetry, String reason, long atMs, int attempts) {
    }

    public DeadLetterLogger(Path file, ObjectMapper objectMapper, int memoryCapacity) {
        this.file = file;
        this.objectMapper = objectMapper;
        this.memoryCapacity = memoryCapacity;
    }

    public synchronized void record(TelemetryPoint t, String reason, int attempts) {
        DeadLetter dl = new DeadLetter(t, reason, System.currentTimeMillis(), attempts);
        if (inMemory.size() < memoryCapacity) {
            inMemory.add(dl);
        }
        if (file != null) {
            try {
                if (file.getParent() != null) {
                    Files.createDirectories(file.getParent());
                }
                try (Writer w = new OutputStreamWriter(
                        Files.newOutputStream(file, StandardOpenOption.CREATE, StandardOpenOption.APPEND),
                        StandardCharsets.UTF_8)) {
                    w.write(objectMapper.writeValueAsString(dl) + "\n");
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public synchronized List<DeadLetter> deadLetters() {
        return List.copyOf(inMemory);
    }

    public synchronized int count() {
        return inMemory.size();
    }
}
