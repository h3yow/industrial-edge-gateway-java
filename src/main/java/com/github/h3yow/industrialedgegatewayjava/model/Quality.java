package com.github.h3yow.industrialedgegatewayjava.model;

/**
 * 点位读数质量。GOOD 才是正常数据；其余均为异常状态，必须被显式记录而不是悄悄吞掉。
 */
public enum Quality {
    GOOD,
    BAD,
    UNCERTAIN
}
