package com.github.h3yow.industrialedgegatewayjava.delivery;

/**
 * 令牌桶限流器（线程安全）。
 *
 * <p>按固定速率补充令牌、桶容量有上限。{@link #tryAcquire()} 在令牌不足时
 * 立即返回 false，不阻塞——调用方据此转入“缓存或拒绝”策略，这也是
 * RATE_LIMITED 与 DOWNSTREAM_DROPPED 两种拒绝原因可区分的依据之一。
 */
public class TokenBucketRateLimiter {

    private final double permitsPerSecond;
    private final double capacity;
    private double tokens;
    private long lastRefillNanos;

    public TokenBucketRateLimiter(double permitsPerSecond, int capacity) {
        if (permitsPerSecond <= 0 || capacity <= 0) {
            throw new IllegalArgumentException("rate and capacity must be positive");
        }
        this.permitsPerSecond = permitsPerSecond;
        this.capacity = capacity;
        this.tokens = capacity;
        this.lastRefillNanos = System.nanoTime();
    }

    public synchronized boolean tryAcquire() {
        refill();
        if (tokens >= 1.0d) {
            tokens -= 1.0d;
            return true;
        }
        return false;
    }

    private void refill() {
        long now = System.nanoTime();
        double elapsedSeconds = (now - lastRefillNanos) / 1_000_000_000.0d;
        if (elapsedSeconds > 0) {
            tokens = Math.min(capacity, tokens + elapsedSeconds * permitsPerSecond);
            lastRefillNanos = now;
        }
    }

    public synchronized double availableTokens() {
        refill();
        return tokens;
    }
}
