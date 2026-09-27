package com.github.h3yow.industrialedgegatewayjava.sim;

import com.github.h3yow.industrialedgegatewayjava.config.GatewayProperties;
import com.github.h3yow.industrialedgegatewayjava.ingest.IngestionService;
import com.github.h3yow.industrialedgegatewayjava.metrics.GatewayMetrics;
import com.github.h3yow.industrialedgegatewayjava.model.Quality;
import com.github.h3yow.industrialedgegatewayjava.model.RawReading;
import com.github.h3yow.industrialedgegatewayjava.session.SessionManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 本地模拟设备运行器：默认关闭；开启后周期创建多台设备并上报读数。
 * 传输层负责先在网关上建立/恢复会话，再调用采集服务。
 */
@Component
public class SimulatorRunner {

    private final GatewayProperties props;
    private final IngestionService ingestion;
    private final SessionManager sessions;
    private final GatewayMetrics metrics;
    private final List<SimulatedDevice> devices = new ArrayList<>();
    private ScheduledExecutorService executor;

    public SimulatorRunner(GatewayProperties props, IngestionService ingestion,
                           SessionManager sessions, GatewayMetrics metrics) {
        this.props = props;
        this.ingestion = ingestion;
        this.sessions = sessions;
        this.metrics = metrics;
    }

    @PostConstruct
    public void start() {
        if (!props.isSimulatorEnabled()) {
            return;
        }
        String[][] specs = {
                {"device-A", "temp_c", "C"},
                {"device-B", "temp_f", "F"},
                {"device-C", "pressure_kpa", "kPa"}
        };
        for (int i = 0; i < Math.min(props.getSimulatorDevices(), specs.length); i++) {
            String[] spec = specs[i];
            String id = spec[0];
            SimulatedDevice device = new SimulatedDevice(id, spec[1], spec[2], 50, this::transport);
            boolean reconnect = sessions.connect(id);
            if (reconnect) {
                metrics.recordReconnect();
            }
            device.connect();
            devices.add(device);
        }
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "simulator-runner");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleAtFixedRate(this::tick, 0, props.getSimulatorIntervalMs(), TimeUnit.MILLISECONDS);
    }

    /** 模拟传输：建连 → 上报。重复/乱序语义由设备与网关两侧共同保证。 */
    private void transport(RawReading reading) {
        if (sessions.session(reading.deviceId()).status()
                == com.github.h3yow.industrialedgegatewayjava.model.SessionStatus.DISCONNECTED) {
            boolean reconnect = sessions.connect(reading.deviceId());
            if (reconnect) {
                metrics.recordReconnect();
            }
        }
        ingestion.ingest(reading);
    }

    private void tick() {
        long now = System.currentTimeMillis();
        for (int i = 0; i < devices.size(); i++) {
            SimulatedDevice d = devices.get(i);
            double value = 20 + i * 5 + Math.sin(now / 1000.0 + i) * 2;
            Quality q = (now / 5000 + i) % 17 == 0 ? Quality.BAD : Quality.GOOD;
            d.emit(value, q, 0);
        }
    }

    /** 供 REST 控制：手动断开某台模拟设备（演示断线）。 */
    public void disconnect(String deviceId) {
        sessions.disconnect(deviceId);
        devices.stream().filter(d -> d.deviceId().equals(deviceId)).findFirst()
                .ifPresent(SimulatedDevice::disconnect);
    }

    /** 供 REST 控制：手动重连（触发离线缓存回放）。 */
    public void reconnect(String deviceId) {
        boolean reconnect = sessions.connect(deviceId);
        if (reconnect) {
            metrics.recordReconnect();
        }
        devices.stream().filter(d -> d.deviceId().equals(deviceId)).findFirst()
                .ifPresent(SimulatedDevice::connect);
    }

    public List<String> deviceIds() {
        return devices.stream().map(SimulatedDevice::deviceId).toList();
    }

    @PreDestroy
    public void stop() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }
}
