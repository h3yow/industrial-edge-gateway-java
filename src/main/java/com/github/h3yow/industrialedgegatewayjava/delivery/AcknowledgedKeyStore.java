package com.github.h3yow.industrialedgegatewayjava.delivery;

import java.util.List;

/**
 * 已确认投递幂等键的持久化留存。
 *
 * <p>重启后 {@link #load()} 恢复集合，使重启前已确认的事件在重复投递时
 * 仍被判为 DUPLICATE_DELIVERY，而不会再次打到下游——重复投递结果可辨认、不翻倍。
 */
public interface AcknowledgedKeyStore {

    /** 追加一个已确认键（重复键幂等）。 */
    void append(String dedupKey);

    /** 读取全部已确认键。 */
    List<String> load();
}
