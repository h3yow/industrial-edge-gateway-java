package com.github.h3yow.industrialedgegatewayjava.downstream;

/**
 * Sink 投递结果。
 *
 * @param accepted  是否被下游接受
 * @param duplicate 是否因幂等去重被识别为重复投递（accepted=true，但不重复计入下游数据）
 * @param detail    解释细节
 */
public record SinkResult(boolean accepted, boolean duplicate, String detail) {
    public static SinkResult ok() {
        return new SinkResult(true, false, "delivered");
    }

    public static SinkResult duplicate(String id) {
        return new SinkResult(true, true, "duplicate delivery ignored: " + id);
    }

    public static SinkResult fail(String detail) {
        return new SinkResult(false, false, detail);
    }
}
