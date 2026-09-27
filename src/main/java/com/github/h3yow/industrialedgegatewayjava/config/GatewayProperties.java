package com.github.h3yow.industrialedgegatewayjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 网关可调参数（前缀 gateway.*）。
 */
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    /** 数据目录（状态快照、WAL、死信）。 */
    private String dataDir = "target/gateway-data";

    /** 是否启用本地模拟设备周期上报。 */
    private boolean simulatorEnabled = false;

    /** 模拟设备数量。 */
    private int simulatorDevices = 3;

    /** 模拟设备上报间隔毫秒。 */
    private long simulatorIntervalMs = 200;

    /** 每台设备乱序 pending 窗口上限。 */
    private int sessionMaxPending = 100;

    /** 原始读数归档容量。 */
    private int archiveCapacity = 10_000;

    /** 下游限流：每秒许可数。 */
    private double ratePermitsPerSecond = 50;

    /** 下游限流：突发桶容量。 */
    private double rateBurstCapacity = 50;

    /** 下游有界投递队列容量。 */
    private int bufferCapacity = 100;

    /** WAL 未确认记录上限（缓存拒绝的第二道上限）。 */
    private long walMaxPending = 1000;

    /** 投递失败最大尝试次数。 */
    private int deliveryMaxAttempts = 3;

    /** 投递重试间隔毫秒。 */
    private long deliveryRetryBackoffMs = 50;

    /** 死信内存保留上限。 */
    private int deadLetterMemoryCapacity = 1000;

    /** 每处理一条读数是否立即刷盘状态快照（本地演示默认开启）。 */
    private boolean flushStatePerRead = true;

    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }

    public boolean isSimulatorEnabled() { return simulatorEnabled; }
    public void setSimulatorEnabled(boolean v) { this.simulatorEnabled = v; }

    public int getSimulatorDevices() { return simulatorDevices; }
    public void setSimulatorDevices(int v) { this.simulatorDevices = v; }

    public long getSimulatorIntervalMs() { return simulatorIntervalMs; }
    public void setSimulatorIntervalMs(long v) { this.simulatorIntervalMs = v; }

    public int getSessionMaxPending() { return sessionMaxPending; }
    public void setSessionMaxPending(int v) { this.sessionMaxPending = v; }

    public int getArchiveCapacity() { return archiveCapacity; }
    public void setArchiveCapacity(int v) { this.archiveCapacity = v; }

    public double getRatePermitsPerSecond() { return ratePermitsPerSecond; }
    public void setRatePermitsPerSecond(double v) { this.ratePermitsPerSecond = v; }

    public double getRateBurstCapacity() { return rateBurstCapacity; }
    public void setRateBurstCapacity(double v) { this.rateBurstCapacity = v; }

    public int getBufferCapacity() { return bufferCapacity; }
    public void setBufferCapacity(int v) { this.bufferCapacity = v; }

    public long getWalMaxPending() { return walMaxPending; }
    public void setWalMaxPending(long v) { this.walMaxPending = v; }

    public int getDeliveryMaxAttempts() { return deliveryMaxAttempts; }
    public void setDeliveryMaxAttempts(int v) { this.deliveryMaxAttempts = v; }

    public long getDeliveryRetryBackoffMs() { return deliveryRetryBackoffMs; }
    public void setDeliveryRetryBackoffMs(long v) { this.deliveryRetryBackoffMs = v; }

    public int getDeadLetterMemoryCapacity() { return deadLetterMemoryCapacity; }
    public void setDeadLetterMemoryCapacity(int v) { this.deadLetterMemoryCapacity = v; }

    public boolean isFlushStatePerRead() { return flushStatePerRead; }
    public void setFlushStatePerRead(boolean v) { this.flushStatePerRead = v; }
}
