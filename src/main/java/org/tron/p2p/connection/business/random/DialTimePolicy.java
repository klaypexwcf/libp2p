package org.tron.p2p.connection.business.random;

public class DialTimePolicy {

    /**
     * 30s 基础网格
     */
    private final long gridMillis;

    /**
     * 同一节点两次尝试之间的最小间隔，默认 60s
     */
    private final long minAttemptIntervalMillis;

    /**
     * 最小提前量。
     * 如果距离某个目标网格点已经小于这个值，则认为来不及准时发起，
     * 直接推迟到下一个 30s 网格点。
     */
    private final long minLeadMillis;

    public DialTimePolicy(long gridMillis, long minAttemptIntervalMillis, long minLeadMillis) {
        if (gridMillis <= 0) {
            throw new IllegalArgumentException("gridMillis must be > 0");
        }
        if (minAttemptIntervalMillis <= 0) {
            throw new IllegalArgumentException("minAttemptIntervalMillis must be > 0");
        }
        if (minLeadMillis < 0) {
            throw new IllegalArgumentException("minLeadMillis must be >= 0");
        }

        this.gridMillis = gridMillis;
        this.minAttemptIntervalMillis = minAttemptIntervalMillis;
        this.minLeadMillis = minLeadMillis;
    }

    /**
     * 计算下一次连接尝试时间。
     *
     * 规则：
     * 1. 尝试时间必须落在 t + 30k 的绝对网格上
     * 2. 不得早于 now + minLeadMillis
     * 3. 不得早于 cooldownUntil
     * 4. 不得早于 lastAttemptAt + minAttemptIntervalMillis
     *
     * 这样可以保证：
     * - 如果错过当前 60s 点，就会自动推到下一个 t+30s 点
     * - 如果那个 t+30s 点会违反 60s 限制，就继续顺延
     * - 始终不会产生累计漂移
     */
    public long nextAttemptTime(long baseTimeMillis,
                                long now,
                                long cooldownUntil,
                                long lastAttemptAt) {

        long earliest = now + minLeadMillis;

        if (cooldownUntil > earliest) {
            earliest = cooldownUntil;
        }

        if (lastAttemptAt > 0) {
            long minNextByInterval = lastAttemptAt + minAttemptIntervalMillis;
            if (minNextByInterval > earliest) {
                earliest = minNextByInterval;
            }
        }

        return ceilToGrid(baseTimeMillis, earliest);
    }

    /**
     * 将 referenceTime 向上对齐到以 baseTimeMillis 为基准的 30s 网格点。
     */
    public long ceilToGrid(long baseTimeMillis, long referenceTime) {
        if (referenceTime <= baseTimeMillis) {
            return baseTimeMillis;
        }

        long delta = referenceTime - baseTimeMillis;
        long k = (delta + gridMillis - 1) / gridMillis; // 向上取整
        return baseTimeMillis + k * gridMillis;
    }

    /**
     * 返回某个时间点对应的网格序号。
     * t -> 0
     * t+30s -> 1
     * t+60s -> 2
     */
    public long gridIndex(long baseTimeMillis, long timeMillis) {
        if (timeMillis < baseTimeMillis) {
            return -1L;
        }
        return (timeMillis - baseTimeMillis) / gridMillis;
    }

    /**
     * 判断一个时间点是否严格落在 t + 30k 网格上。
     */
    public boolean isExactGridTime(long baseTimeMillis, long timeMillis) {
        if (timeMillis < baseTimeMillis) {
            return false;
        }
        return (timeMillis - baseTimeMillis) % gridMillis == 0;
    }

    public long getGridMillis() {
        return gridMillis;
    }

    public long getMinAttemptIntervalMillis() {
        return minAttemptIntervalMillis;
    }

    public long getMinLeadMillis() {
        return minLeadMillis;
    }
}