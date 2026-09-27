package com.github.h3yow.industrialedgegatewayjava.model;

/**
 * 设备上报的一条原始点位读数（协议无关）。
 *
 * @param deviceId   设备唯一标识
 * @param pointCode  设备侧原始点位编码（协议侧名称）
 * @param value      原始数值
 * @param unit       原始单位
 * @param timestamp  设备时间戳（epoch 毫秒，UTC 基准）
 * @param sequence   设备内单调递增序列号，用于去重/乱序判定
 * @param quality    设备侧质量码（GOOD 为正常）
 * @param duplicate  本次上报是否为设备重发（连接层标记，非业务判定）
 */
public record RawReading(String deviceId,
                         String pointCode,
                         double value,
                         String unit,
                         long timestamp,
                         long sequence,
                         String quality,
                         boolean duplicate) {
}
