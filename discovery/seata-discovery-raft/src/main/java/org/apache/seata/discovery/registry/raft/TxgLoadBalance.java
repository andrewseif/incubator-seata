package org.apache.seata.discovery.registry.raft;

import org.apache.seata.core.context.RootContext;
import org.apache.seata.discovery.loadbalance.LoadBalance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
/**
 * TXG-based LoadBalance implementation
 * Uses RootContext to check for raft group information, falls back to random selection
 */
public class TxgLoadBalance implements LoadBalance {

    private static final Logger LOGGER = LoggerFactory.getLogger(TxgLoadBalance.class);

    // Custom context key for raft group
    private static final String KEY_RAFT_GROUP = "TX_RAFT_GROUP";

    @Override
    public <T> T select(List<T> invokers, String xid) throws Exception {
        if (invokers == null || invokers.isEmpty()) {
            return null;
        }

        if (invokers.size() == 1) {
            return invokers.get(0);
        }

        // Check RootContext for raft group
        String raftGroup = getRaftGroupFromContext();

        if (raftGroup != null) {
            // Get leader for specific TXG
            InetSocketAddress leader = RaftRegistryServiceImpl.getTxgLeader(raftGroup);
            if (leader != null) {
                // Find matching invoker
                for (T invoker : invokers) {
                    if (matchesAddress(invoker, leader)) {
                        LOGGER.debug("Selected TXG leader for group {}: {}", raftGroup, leader);
                        return invoker;
                    }
                }
            }
        }

        // Fallback to random selection
        int index = ThreadLocalRandom.current().nextInt(invokers.size());
        T selected = invokers.get(index);
        LOGGER.debug("No raft group specified, random selection: {}", selected);
        return selected;
    }

    /**
     * Get raft group from RootContext
     */
    private String getRaftGroupFromContext() {
        try {
            return (String) RootContext.entries().get(KEY_RAFT_GROUP);
        } catch (Exception e) {
            LOGGER.debug("Failed to get raft group from context", e);
            return null;
        }
    }

    /**
     * Check if invoker matches the target address
     */
    private boolean matchesAddress(Object invoker, InetSocketAddress target) {
        if (invoker instanceof InetSocketAddress) {
            InetSocketAddress addr = (InetSocketAddress) invoker;
            return addr.getHostString().equals(target.getHostString()) &&
                    addr.getPort() == target.getPort();
        }
        return false;
    }

    /**
     * Bind raft group to RootContext (utility method for applications)
     */
    public static void bindRaftGroup(String raftGroup) {
        RootContext.entries().put(KEY_RAFT_GROUP, raftGroup);
        LOGGER.debug("Bound raft group: {}", raftGroup);
    }

    /**
     * Unbind raft group from RootContext
     */
    public static void unbindRaftGroup() {
        String removed = (String) RootContext.entries().remove(KEY_RAFT_GROUP);
        if (removed != null) {
            LOGGER.debug("Unbound raft group: {}", removed);
        }
    }
}

