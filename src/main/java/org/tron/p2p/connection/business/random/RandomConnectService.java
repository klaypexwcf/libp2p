package org.tron.p2p.connection.business.random;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.tron.p2p.connection.Channel;
import org.tron.p2p.connection.ChannelManager;
import org.tron.p2p.connection.socket.PeerClient;
import org.tron.p2p.discover.Node;
import org.tron.p2p.protos.Connect.DisconnectReason;

@Slf4j(topic = "Random")
public class RandomConnectService {

    private final PeerClient peerClient;
    private final TargetStateRepo targetStateRepo;
    private final DialTimePolicy dialTimePolicy;

    private final long cooldownMillis;
    private final long scanIntervalMillis;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    private volatile boolean started = false;

    private final ScheduledExecutorService statsScheduler =
            Executors.newSingleThreadScheduledExecutor();

    public RandomConnectService(PeerClient peerClient,
                                TargetStateRepo targetStateRepo,
                                DialTimePolicy dialTimePolicy,
                                long cooldownMillis,
                                long scanIntervalMillis) {
        this.peerClient = peerClient;
        this.targetStateRepo = targetStateRepo;
        this.dialTimePolicy = dialTimePolicy;
        this.cooldownMillis = cooldownMillis;
        this.scanIntervalMillis = scanIntervalMillis;
    }

    /**
     * 初始化预设目标节点。
     * 当前版本：未指定 t 时，直接设为当前时间。
     */
    public void init(List<InetSocketAddress> targetAddresses) {
        long now = System.currentTimeMillis();

        for (InetSocketAddress address : targetAddresses) {
            if (address == null) {
                continue;
            }

            Node node = new Node(address);
            String key = TargetPeerState.buildKey(node);

            if (targetStateRepo.contains(key)) {
                continue;
            }

            long baseTimeMillis = now;

            TargetPeerState state = new TargetPeerState(key, node, baseTimeMillis);
            long nextAttemptAt = dialTimePolicy.nextAttemptTime(
                    state.getBaseTimeMillis(),
                    now,
                    state.getCooldownUntil(),
                    state.getLastAttemptAt()
            );
            state.setNextAttemptAt(nextAttemptAt);

            targetStateRepo.put(state);

            log.info("Init random target, key={}, t={}, nextAttemptAt={}",
                    key, baseTimeMillis, nextAttemptAt);
        }
    }

    public void start() {
        if (started) {
            return;
        }
        started = true;

        scheduler.scheduleAtFixedRate(
                this::safeScanAndConnect,
                0,
                scanIntervalMillis,
                TimeUnit.MILLISECONDS
        );
        startStatsTask();

        log.info("RandomConnectService started");
    }

    public void stop() {
        started = false;
        scheduler.shutdownNow();
        stopStatsTask();
        log.info("RandomConnectService stopped");
    }

    private void startStatsTask() {
        statsScheduler.scheduleAtFixedRate(
                this::safeLogRandomStats,
                0,
                150,   // 2.5 min = 150 s
                TimeUnit.SECONDS
        );
    }

    private void stopStatsTask() {
        statsScheduler.shutdownNow();
    }
    private void safeLogRandomStats() {
        try {
            logRandomStats();
        } catch (Throwable t) {
            log.error("logRandomStats error", t);
        }
    }
    private void logRandomStats() {
        Set<String> connectedIps = new TreeSet<>();
        Set<String> connectedRandomEliIps = new TreeSet<>();

        Set<String> unconnectedIps = new TreeSet<>();
        Set<String> unconnectedRandomEliIps = new TreeSet<>();

        for (TargetPeerState state : targetStateRepo.all()) {
            String ip = targetIp(state);
            if (ip == null) {
                continue;
            }

            if (state.isConnected()) {
                connectedIps.add(ip);
                if (state.getLastRandomEliAt() > 0) {
                    connectedRandomEliIps.add(ip);
                }
            } else {
                unconnectedIps.add(ip);
                if (state.getLastRandomEliAt() > 0) {
                    unconnectedRandomEliIps.add(ip);
                }
            }
        }

        log.info("RANDOM_CONNECTED total={}, ips={}, randomEliIps={}",
                connectedIps.size(), connectedIps, connectedRandomEliIps);

        log.info("RANDOM_UNCONNECTED total={}, ips={}, randomEliIps={}",
                unconnectedIps.size(), unconnectedIps, unconnectedRandomEliIps);
    }

    private String targetIp(TargetPeerState state) {
        if (state == null
                || state.getNode() == null
                || state.getNode().getPreferInetSocketAddress() == null
                || state.getNode().getPreferInetSocketAddress().getAddress() == null) {
            return null;
        }
        return state.getNode().getPreferInetSocketAddress().getAddress().getHostAddress();
    }
    private void safeScanAndConnect() {
        try {
            scanAndConnect();
        } catch (Throwable t) {
            log.error("scanAndConnect error", t);
        }
    }

    private void scanAndConnect() {
        long now = System.currentTimeMillis();

        for (TargetPeerState state : targetStateRepo.all()) {
            boolean shouldDial;
            synchronized (state) {
                shouldDial = state.canDialNow(now);
                if (shouldDial) {
                    state.markDialing(now);
                }
            }

            if (!shouldDial) {
                continue;
            }

            doConnect(state);
        }
    }

    /**
     * 用你新增的 connectManagedAsync 发起连接。
     */
    private void doConnect(TargetPeerState state) {
        Node node = state.getNode();

        log.info("Random connect to {}", node.getPreferInetSocketAddress());

        ChannelFuture future = peerClient.connectManagedAsync(node, (ChannelFutureListener) f -> {
            long now = System.currentTimeMillis();

            if (!f.isSuccess()) {
                log.warn("Random connect fail, peer={}, cause={}",
                        node.getPreferInetSocketAddress(),
                        f.cause() == null ? "unknown" : f.cause().getMessage());

                onConnectFail(state.getKey(), now);

                if (f.channel() != null) {
                    f.channel().close();
                }
            } else {
                onConnectSuccess(state.getKey(), now);

                // 连接建立成功后，监听后续断开
                f.channel().closeFuture().addListener((ChannelFutureListener) closeFuture -> {
                    long disconnectTime = System.currentTimeMillis();
                    DisconnectReason reason = resolveDisconnectReason(state);
                    onDisconnected(state.getKey(), reason, disconnectTime);
                });
            }
        });

        if (future == null) {
            onConnectFail(state.getKey(), System.currentTimeMillis());
        }
    }

    public void onConnectSuccess(String key, long eventTimeMillis) {
        TargetPeerState state = targetStateRepo.get(key);
        if (state == null) {
            return;
        }

        synchronized (state) {
            state.markConnected(eventTimeMillis);
        }

        log.info("Random connect success, key={}, time={}", key, eventTimeMillis);
    }

    public void onConnectFail(String key, long eventTimeMillis) {
        TargetPeerState state = targetStateRepo.get(key);
        if (state == null) {
            return;
        }

        synchronized (state) {
            state.markConnectFail(eventTimeMillis, cooldownMillis);

            long nextAttemptAt = dialTimePolicy.nextAttemptTime(
                    state.getBaseTimeMillis(),
                    eventTimeMillis,
                    state.getCooldownUntil(),
                    state.getLastAttemptAt()
            );
            state.setNextAttemptAt(nextAttemptAt);

            log.info("Random connect fail handled, key={}, cooldownUntil={}, nextAttemptAt={}",
                    key, state.getCooldownUntil(), state.getNextAttemptAt());
        }
    }

    public void onDisconnected(String key, DisconnectReason reason, long eventTimeMillis) {
        TargetPeerState state = targetStateRepo.get(key);
        if (state == null) {
            return;
        }

        synchronized (state) {
            state.markDisconnected(eventTimeMillis, cooldownMillis);

            // 可选：如果你给 TargetPeerState 加了这两个字段，可以顺手记一下
            // state.setLastDisconnectedAt(eventTimeMillis);
            // state.setLastDisconnectReason(reason);

            if (reason == DisconnectReason.RANDOM_ELIMINATION) {
                state.markRandomEli(eventTimeMillis);
            }

            long nextAttemptAt = dialTimePolicy.nextAttemptTime(
                    state.getBaseTimeMillis(),
                    eventTimeMillis,
                    state.getCooldownUntil(),
                    state.getLastAttemptAt()
            );
            state.setNextAttemptAt(nextAttemptAt);

            log.info("Random disconnect handled, key={}, reason={}, t={}, cooldownUntil={}, nextAttemptAt={}",
                    key,
                    reason,
                    state.getBaseTimeMillis(),
                    state.getCooldownUntil(),
                    state.getNextAttemptAt());
        }
    }

    /**
     * 按你的思路：
     * 通过 ChannelManager 中保存的 channels 表，
     * 用 InetSocketAddress 反查业务 Channel，再读取 disconnectReason。
     */
    private DisconnectReason resolveDisconnectReason(TargetPeerState state) {
        if (state == null || state.getNode() == null || state.getNode().getPreferInetSocketAddress() == null) {
            return DisconnectReason.UNKNOWN;
        }

        InetSocketAddress address = state.getNode().getPreferInetSocketAddress();
        Channel channel = ChannelManager.getChannel(address);

        if (channel == null || channel.getDisconnectReason() == null) {
            return DisconnectReason.UNKNOWN;
        }

        return channel.getDisconnectReason();
    }

    /**
     * 手工刷新某个节点的 nextAttemptAt
     */
    public void refreshNextAttempt(String key) {
        TargetPeerState state = targetStateRepo.get(key);
        if (state == null) {
            return;
        }

        long now = System.currentTimeMillis();

        synchronized (state) {
            long nextAttemptAt = dialTimePolicy.nextAttemptTime(
                    state.getBaseTimeMillis(),
                    now,
                    state.getCooldownUntil(),
                    state.getLastAttemptAt()
            );
            state.setNextAttemptAt(nextAttemptAt);
        }
    }
}