package com.github.h3yow.industrialedgegatewayjava.model;

/**
 * 序列号空洞：说明 [firstMissing, lastSeen] 之间存在未到达读数，
 * 属于异常事件，必须显式呈现，不能当成正常数据悄悄吞掉。
 *
 * @param deviceId    设备
 * @param firstMissing 第一个缺失序列号
 * @param lastMissing  最后一个缺失序列号
 * @param detectedAtMs 检测时间
 */
public record GapAnomaly(
        String deviceId,
        long firstMissing,
        long lastMissing,
        long detectedAtMs
) {
}
