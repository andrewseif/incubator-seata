package org.apache.seata.discovery.registry.raft.dto;

/**
 * Load balancing strategies for TXG selection
 */
public enum LoadBalanceStrategy {

    ROUND_ROBIN("round_robin", "Round robin selection"),
    WEIGHTED("weighted", "Weighted selection based on TXG capacity"),
    LEAST_CONNECTIONS("least_connections", "Select TXG with least active connections"),
    RANDOM("random", "Random selection"),
    STICKY("sticky", "Sticky selection based on service group hash");

    private final String code;
    private final String description;

    LoadBalanceStrategy(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public String getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    /**
     * Get strategy by code string
     */
    public static LoadBalanceStrategy fromCode(String code) {
        if (code == null) {
            return ROUND_ROBIN; // default
        }

        for (LoadBalanceStrategy strategy : values()) {
            if (strategy.code.equalsIgnoreCase(code)) {
                return strategy;
            }
        }

        return ROUND_ROBIN; // default fallback
    }

    @Override
    public String toString() {
        return code;
    }
}
