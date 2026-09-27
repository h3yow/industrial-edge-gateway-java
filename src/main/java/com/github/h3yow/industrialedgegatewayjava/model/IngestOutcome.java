package com.github.h3yow.industrialedgegatewayjava.model;

/**
 * 采集入口对一条原始读数的判定结果分类。
 */
public enum IngestOutcome {
    /** 正常归一化并进入投递 */
    ACCEPTED,
    /** 重复上报：相同序列号已处理过，不重复累计 */
    DUPLICATE,
    /** 乱序：序列号落后于已确认水位，且不是重复 */
    OUT_OF_ORDER,
    /** 异常数据：质量码、时间戳、未知点位等，不参与累计 */
    REJECTED
}
