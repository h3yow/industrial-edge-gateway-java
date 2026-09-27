package com.github.h3yow.industrialedgegatewayjava.delivery;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * 本地文件实现：每个已确认键一行（append-only）。
 * 仅依赖本地文件系统，无外部服务；重复行在 load 时去重。
 */
public class FileAcknowledgedKeyStore implements AcknowledgedKeyStore {

    private final Path file;

    public FileAcknowledgedKeyStore(Path file) {
        this.file = file;
        try {
            if (file.getParent() != null) {
                Files.createDirectories(file.getParent());
            }
            if (!Files.exists(file)) {
                Files.createFile(file);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot init key store " + file, e);
        }
    }

    @Override
    public synchronized void append(String dedupKey) {
        try {
            Files.writeString(file, dedupKey + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("key store append failed", e);
        }
    }

    @Override
    public synchronized List<String> load() {
        try {
            return Files.readAllLines(file, StandardCharsets.UTF_8).stream()
                    .filter(s -> !s.isBlank())
                    .distinct()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("key store load failed", e);
        }
    }
}
