package org.tron.p2p.connection.business.random;

import lombok.Getter;
import lombok.Setter;
import org.tron.p2p.discover.Node;

@Getter
@Setter
public class TargetPeerState {

    /**
     * 稳定唯一标识，建议使用 host:port
     */
    private final String key;

    /**
     * 目标节点对象，直接引用现有 Node
     */
    private final Node node;

    /**
     * 当前基准时间 t
     * 初始化时：
     *   - 若配置中指定，则用配置值
     *   - 若未指定，则用当前时间
     * RandomELi 断开后会更新为事件发生时间
     */
    private volatile long baseTimeMillis;

    /**
     * 下一个应该发起连接尝试的时间点
     * 由 DialTimePolicy 计算得到
     *
     * 注意：
     * 它应该始终对齐在以 t 为基准的 30s 网格点上，
     * 例如 t, t+30s, t+60s, t+90s ...
     */
    private volatile long nextAttemptAt = 0L;

    /**
     * 当前是否已建立连接
     */
    private volatile boolean connected = false;

    /**
     * 当前是否正在发起连接，防止重复提交 connect
     */
    private volatile boolean dialing = false;

    /**
     * 最近一次“实际发起连接尝试”的时间
     * 用于保证同一节点 60s 内不尝试两次
     */
    private volatile long lastAttemptAt = 0L;

    /**
     * 冷却截止时间
     * now < cooldownUntil 时，不允许再次连接
     */
    private volatile long cooldownUntil = 0L;

    /**
     * 最近一次连接成功的时间
     */
    private volatile long lastConnectedAt = 0L;

    /**
     * 最近一次 RandomELi 事件时间
     */
    private volatile long lastRandomEliAt = 0L;

    public TargetPeerState(String key, Node node, long baseTimeMillis) {
        this.key = key;
        this.node = node;
        this.baseTimeMillis = baseTimeMillis;
    }

    /**
     * 当前是否处于冷却期
     */
    public boolean isInCooldown(long now) {
        return now < cooldownUntil;
    }

    /**
     * 仅从本地状态判断“是否允许拨号”
     * 不检查 nextAttemptAt 是否到点
     *
     * 这个方法适合给 service 内部复用：
     * service 先判断状态是否允许，再判断 now >= nextAttemptAt
     */
    public boolean canDialByState(long now) {
        return !connected && !dialing && now >= cooldownUntil;
    }

    /**
     * 当前是否已经到达应当尝试连接的时间点
     */
    public boolean isAttemptDue(long now) {
        return nextAttemptAt > 0 && now >= nextAttemptAt;
    }

    /**
     * 当前是否可以立刻发起连接
     */
    public boolean canDialNow(long now) {
        return canDialByState(now) && isAttemptDue(now);
    }

    /**
     * 标记开始发起连接
     */
    public void markDialing(long now) {
        this.dialing = true;
        this.lastAttemptAt = now;
    }

    /**
     * 标记连接成功
     */
    public void markConnected(long now) {
        this.connected = true;
        this.dialing = false;
        this.lastConnectedAt = now;
    }

    /**
     * 标记连接失败，进入冷却
     */
    public void markConnectFail(long now, long cooldownMillis) {
        this.connected = false;
        this.dialing = false;
        this.cooldownUntil = now + cooldownMillis;
    }

    /**
     * 标记连接断开，进入冷却
     */
    public void markDisconnected(long now, long cooldownMillis) {
        this.connected = false;
        this.dialing = false;
        this.cooldownUntil = now + cooldownMillis;
    }

    /**
     * RandomELi 断开时调用：
     * 1. 记录事件时间
     * 2. 将 t 对齐到该事件时间
     */
    public void markRandomEli(long eventTimeMillis) {
        this.lastRandomEliAt = eventTimeMillis;
        this.baseTimeMillis = eventTimeMillis;
    }

    /**
     * 重置为“未连接且未在拨号”的状态
     * 一般用于保险清理，不主动修改 cooldown
     */
    public void resetTransientState() {
        this.connected = false;
        this.dialing = false;
    }

    /**
     * 手工构造 key，建议统一在一个地方调用
     */
    public static String buildKey(Node node) {
        if (node == null || node.getPreferInetSocketAddress() == null
                || node.getPreferInetSocketAddress().getAddress() == null) {
            throw new IllegalArgumentException("node or address is null");
        }
        return node.getPreferInetSocketAddress().getAddress().getHostAddress()
                + ":" + node.getPort();
    }
}