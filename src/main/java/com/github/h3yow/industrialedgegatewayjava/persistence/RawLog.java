package com.github.h3yow.industrialedgegatewayjava.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.industrialedgegatewayjava.model.RawReading;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 原始读数的本地 JSON-Lines 留存（append-only）。
 *
 * <p>只追加、不修改：映射规则升级后，可用指定历史版本对这份留存做确定性回放，
 * 得到可复现的结论（相同输入 + 相同版本 = 相同输出）。
 * 损坏行会被跳过，不影响其余数据的回放。
 */
public class RawLog {

    private final Path file;
    private final ObjectMapper mapper = new ObjectMapper();

    public RawLog(Path file) {
        this.file = file;
        try {
            if (file.getParent() != null) {
                Files.createDirectories(file.getParent());
            }
            if (!Files.exists(file)) {
                Files.createFile(file);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot init raw log " + file, e);
        }
    }

    public synchronized void append(RawReading reading) {
        try {
            Files.writeString(file, mapper.writeValueAsString(reading) + System.lineSeparator(),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("raw log append failed", e);
        }
    }

    public synchronized List<RawReading> readAll() {
        try {
            List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            List<RawReading> out = new ArrayList<>(lines.size());
            for (String line : lines) {
                if (!line.isBlank()) {
                    out.add(mapper.readValue(line, RawReading.class));
                }
            }
            return out;
        } catch (IOException e) {
            throw new UncheckedIOException("raw log read failed", e);
        }
    }
}
