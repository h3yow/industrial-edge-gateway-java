package com.github.h3yow.industrialedgegatewayjava.mapping;

import java.util.List;

/**
 * 映射规则的一个不可变版本。规则调整 = 发布新版本，旧版本永久保留以支持历史回放复现。
 *
 * @param name  版本名（如 mapping-v1）
 * @param rules 该版本下的全部规则（按声明顺序匹配，先命中先生效）
 */
public record MappingVersion(String name, List<MappingRule> rules) {
    public MappingVersion {
        rules = List.copyOf(rules);
    }
}
