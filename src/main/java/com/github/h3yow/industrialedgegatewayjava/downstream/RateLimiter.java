package com.github.h3yow.industrialedgegatewayjava.downstream;

/**
 * 令牌桶限流器（线程安全）。非阻塞：拿不到令牌立即返回 false（调用方据此判定 RATE_LIMITED）。
 * 限流触发依据可解释：当前令牌数、待入队请求数会出现在判定细节中。
 */
public class RateLimiter {

    private final double capacity;
    private final double refillPerMs;
    private double tokens;
    private long lastRefillMs;

    public RateLimiter(double permitsPerSecond, double burstCapacity) {
        if (permitsPerSecond <= 0 || burstCapacity <= 0) {
            throw new IllegalArgumentException("限流参数必须为正");
        }
        this.capacity = burstCapacity;
        this.refillPerMs = permitsPerSecond / 1000.0;
        this.tokens = burstCapacity;
        this.lastRefillMs = System.currentTimeMillis();
    }

    public synchronized boolean tryAcquire() {
        refill();
        if (tokens >= 1.0d) {
            tokens -= 1.0d;
            return true;
        }
        return false;
    }

    public synchronized double availableTokens() {
        refill();
        return tokens;
    }

    private void refill() {
        long now = System.currentTimeMillis();
        long elapsed = now - lastRefillMs;
        if (elapsed > 0) {
            tokens = Math.min(capacity, tokens + elapsed * refillPerMs);
            lastRefillMs = now;
        }
    }
}
