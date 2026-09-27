package com.github.h3yow.industrialedgegatewayjava.replay;

import com.github.h3yow.industrialedgegatewayjava.model.RawReading;
import com.github.h3yow.industrialedgegatewayjava.model.TelemetryEvent;
import com.github.h3yow.industrialedgegatewayjava.transform.TelemetryTransformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 历史回放。
 *
 * <p>用<b>指定的映射版本</b>对留存的原始读数重新归一化，不做去重、不写状态、不投递下游，
 * 因此是纯函数式的确定性计算：相同输入 + 相同版本 => 相同输出。
 * 在该版本下没有映射的点位，结果中以 null 事件占位（由调用方按 UNKNOWN_POINT 展示），
 * 保证每条输入在回放结论里都有对应行，不会悄悄消失。
 */
public class ReplayService {

    public record ReplayLine(RawReading input, TelemetryEvent output, String note) {
    }

    private final TelemetryTransformer transformer;

    public ReplayService(TelemetryTransformer transformer) {
        this.transformer = transformer;
    }

    public List<TelemetryEvent> replay(List<RawReading> readings, String mappingVersion) {
        List<TelemetryEvent> out = new ArrayList<>(readings.size());
        for (RawReading reading : readings) {
            transformer.transform(reading, reading.timestamp(), mappingVersion)
                    .ifPresent(out::add);
        }
        return out;
    }

    /** 带解释行的回放：未知点位/缺失数据也保留一行。 */
    public List<ReplayLine> replayExplained(List<RawReading> readings, String mappingVersion) {
        List<ReplayLine> out = new ArrayList<>(readings.size());
        for (RawReading reading : readings) {
            Optional<TelemetryEvent> event = transformer.transform(reading, reading.timestamp(), mappingVersion);
            if (event.isPresent()) {
                out.add(new ReplayLine(reading, event.get(), null));
            } else {
                out.add(new ReplayLine(reading, null, "UNKNOWN_POINT in " + mappingVersion));
            }
        }
        return out;
    }
}
