package org.tron.p2p.connection.business.random;

import org.tron.p2p.discover.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TargetStateRepo {

    /**
     * key -> TargetPeerState
     * key 默认使用 host:port
     */
    private final Map<String, TargetPeerState> stateMap = new ConcurrentHashMap<>();

    /**
     * 放入一个状态对象；若 key 已存在则覆盖
     */
    public void put(TargetPeerState state) {
        if (state == null) {
            throw new IllegalArgumentException("state is null");
        }
        stateMap.put(state.getKey(), state);
    }

    /**
     * 仅当 key 不存在时放入
     *
     * @return true 表示放入成功；false 表示已存在
     */
    public boolean putIfAbsent(TargetPeerState state) {
        if (state == null) {
            throw new IllegalArgumentException("state is null");
        }
        return stateMap.putIfAbsent(state.getKey(), state) == null;
    }

    /**
     * 按 key 获取状态
     */
    public TargetPeerState get(String key) {
        if (key == null) {
            return null;
        }
        return stateMap.get(key);
    }

    /**
     * 按 Node 获取状态
     */
    public TargetPeerState get(Node node) {
        if (node == null) {
            return null;
        }
        return stateMap.get(TargetPeerState.buildKey(node));
    }

    /**
     * 按 key 判断是否存在
     */
    public boolean contains(String key) {
        if (key == null) {
            return false;
        }
        return stateMap.containsKey(key);
    }

    /**
     * 按 Node 判断是否存在
     */
    public boolean contains(Node node) {
        if (node == null) {
            return false;
        }
        return stateMap.containsKey(TargetPeerState.buildKey(node));
    }

    /**
     * 不存在时创建并返回；存在时直接返回已有状态
     *
     * 适合初始化阶段或运行中懒加载节点状态
     */
    public TargetPeerState getOrCreate(Node node, long baseTimeMillis) {
        if (node == null) {
            throw new IllegalArgumentException("node is null");
        }

        String key = TargetPeerState.buildKey(node);
        return stateMap.computeIfAbsent(key, k -> new TargetPeerState(k, node, baseTimeMillis));
    }

    /**
     * 按 key 删除
     */
    public TargetPeerState remove(String key) {
        if (key == null) {
            return null;
        }
        return stateMap.remove(key);
    }

    /**
     * 按 Node 删除
     */
    public TargetPeerState remove(Node node) {
        if (node == null) {
            return null;
        }
        return stateMap.remove(TargetPeerState.buildKey(node));
    }

    /**
     * 返回当前所有状态的快照列表
     *
     * 注意：
     * 这里返回的是新的 ArrayList，避免外部直接操作 map 的 values 视图
     */
    public Collection<TargetPeerState> all() {
        return new ArrayList<>(stateMap.values());
    }

    /**
     * 返回只读视图
     * 如果你后面只想遍历、不想让调用方误改集合，可以用这个
     */
    public Collection<TargetPeerState> allReadOnly() {
        return Collections.unmodifiableCollection(new ArrayList<>(stateMap.values()));
    }

    /**
     * 当前节点状态数量
     */
    public int size() {
        return stateMap.size();
    }

    /**
     * 是否为空
     */
    public boolean isEmpty() {
        return stateMap.isEmpty();
    }

    /**
     * 清空仓库
     */
    public void clear() {
        stateMap.clear();
    }
}
