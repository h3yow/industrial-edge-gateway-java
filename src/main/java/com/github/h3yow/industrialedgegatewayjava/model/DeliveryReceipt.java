package com.github.h3yow.industrialedgegatewayjava.model;

/**
 * 投递回执。
 */
public record DeliveryReceipt(DeliveryStatus status, String dedupKey, String reason) {

    public static DeliveryReceipt delivered(String key) {
        return new DeliveryReceipt(DeliveryStatus.DELIVERED, key, null);
    }

    public static DeliveryReceipt duplicate(String key) {
        return new DeliveryReceipt(DeliveryStatus.DUPLICATE_DELIVERY, key, "dedup-key already acknowledged");
    }

    public static DeliveryReceipt buffered(String key) {
        return new DeliveryReceipt(DeliveryStatus.BUFFERED, key, "spooled for retry");
    }

    public static DeliveryReceipt rateLimited(String key) {
        return new DeliveryReceipt(DeliveryStatus.RATE_LIMITED, key, "rate limit exceeded and spool full");
    }

    public static DeliveryReceipt dropped(String key) {
        return new DeliveryReceipt(DeliveryStatus.DOWNSTREAM_DROPPED, key, "downstream unavailable and spool full");
    }
}
