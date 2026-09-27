package com.github.h3yow.industrialedgegatewayjava.session;

import com.github.h3yow.industrialedgegatewayjava.model.RawReading;
import com.github.h3yow.industrialedgegatewayjava.state.DeviceStateStore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 设备会话注册表：按 deviceId 维护会话；会话的序列号水位从持久化状态初始化，
 * 因此服务重启后重复上报依然会被判定为 DUPLICATE，不会二次累计。
 */
public class SessionManager {

    private final DeviceStateStore stateStore;
    private final int maxPending;
    private final Map<String, DeviceSession> sessions = new ConcurrentHashMap<>();

    public SessionManager(DeviceStateStore stateStore, int maxPending) {
        this.stateStore = stateStore;
        this.maxPending = maxPending;
    }

    /**
     * 建立/恢复设备连接。
     *
     * @return true 表示这是一次断线后的重连（首次建连返回 false）
     */
    public boolean connect(String deviceId) {
        DeviceSession s = sessions.computeIfAbsent(deviceId,
                id -> new DeviceSession(id, stateStore.lastContiguous(id), maxPending));
        return s.connect();
    }

    public void disconnect(String deviceId) {
        DeviceSession s = sessions.get(deviceId);
        if (s != null) {
            s.disconnect();
        }
    }

    public DeviceSession session(String deviceId) {
        return sessions.computeIfAbsent(deviceId,
                id -> new DeviceSession(id, stateStore.lastContiguous(id), maxPending));
    }

    public Map<String, DeviceSession> all() {
        return Map.copyOf(sessions);
    }
}
