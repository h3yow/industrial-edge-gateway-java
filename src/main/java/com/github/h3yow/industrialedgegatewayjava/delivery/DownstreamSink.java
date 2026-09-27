package com.github.h3yow.industrialedgegatewayjava.delivery;

import com.github.h3yow.industrialedgegatewayjava.model.TelemetryEvent;

/**
 * 下游投递目标（本地模拟）。send 可阻塞、可标记不可用，用于复现背压。
 */
public interface DownstreamSink {

    void send(TelemetryEvent event) throws InterruptedException;

    boolean available();
}
