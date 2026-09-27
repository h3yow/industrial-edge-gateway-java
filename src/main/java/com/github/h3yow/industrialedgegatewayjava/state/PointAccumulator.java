package com.github.h3yow.industrialedgegatewayjava.state;

/**
 * 单个“设备 + 指标”的累计状态（可变，由 DeviceStateStore 统一加锁）。
 * 只对 GOOD 质量读数累计；BAD/UNCERTAIN 不参与，保证异常不当作正常数据累计。
 */
public class PointAccumulator {

    private long count;
    private double sum;
    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;
    private Double lastValue;
    private long lastEventTimeMs;
    private long lastSequence = -1L;

    public void accept(double value, long eventTimeMs, long sequence) {
        count++;
        sum += value;
        min = Math.min(min, value);
        max = Math.max(max, value);
        this.lastValue = value;
        this.lastEventTimeMs = eventTimeMs;
        this.lastSequence = sequence;
    }

    public long count() {
        return count;
    }

    public double sum() {
        return sum;
    }

    public double min() {
        return count == 0 ? Double.NaN : min;
    }

    public double max() {
        return count == 0 ? Double.NaN : max;
    }

    public double average() {
        return count == 0 ? Double.NaN : sum / count;
    }

    public Double lastValue() {
        return lastValue;
    }

    public long lastEventTimeMs() {
        return lastEventTimeMs;
    }

    public long lastSequence() {
        return lastSequence;
    }

    /** 反序列化用全字段构造。 */
    public static PointAccumulator restore(long count, double sum, double min, double max,
                                           Double lastValue, long lastEventTimeMs, long lastSequence) {
        PointAccumulator a = new PointAccumulator();
        a.count = count;
        a.sum = sum;
        a.min = min;
        a.max = max;
        a.lastValue = lastValue;
        a.lastEventTimeMs = lastEventTimeMs;
        a.lastSequence = lastSequence;
        return a;
    }
}
