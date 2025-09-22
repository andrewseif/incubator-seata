package org.apache.seata.discovery.registry.raft.dto;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * Information about a selected TXG for a service group
 */
public class TxgSelectionInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private String serviceGroup;
    private String selectedTxgId;
    private List<InetSocketAddress> transactionEndpoints;
    private long selectionTime;
    private long cacheExpiry;
    private String selectionStrategy;
    private boolean valid;

    public TxgSelectionInfo() {}

    public TxgSelectionInfo(String serviceGroup, String selectedTxgId, List<InetSocketAddress> transactionEndpoints) {
        this.serviceGroup = serviceGroup;
        this.selectedTxgId = selectedTxgId;
        this.transactionEndpoints = transactionEndpoints;
        this.selectionTime = System.currentTimeMillis();
        this.valid = true;
    }

    // Getters and Setters
    public String getServiceGroup() {
        return serviceGroup;
    }

    public void setServiceGroup(String serviceGroup) {
        this.serviceGroup = serviceGroup;
    }

    public String getSelectedTxgId() {
        return selectedTxgId;
    }

    public void setSelectedTxgId(String selectedTxgId) {
        this.selectedTxgId = selectedTxgId;
    }

    public List<InetSocketAddress> getTransactionEndpoints() {
        return transactionEndpoints;
    }

    public void setTransactionEndpoints(List<InetSocketAddress> transactionEndpoints) {
        this.transactionEndpoints = transactionEndpoints;
    }

    public long getSelectionTime() {
        return selectionTime;
    }

    public void setSelectionTime(long selectionTime) {
        this.selectionTime = selectionTime;
    }

    public long getCacheExpiry() {
        return cacheExpiry;
    }

    public void setCacheExpiry(long cacheExpiry) {
        this.cacheExpiry = cacheExpiry;
    }

    public String getSelectionStrategy() {
        return selectionStrategy;
    }

    public void setSelectionStrategy(String selectionStrategy) {
        this.selectionStrategy = selectionStrategy;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    /**
     * Check if this selection is expired
     */
    public boolean isExpired() {
        return !valid || System.currentTimeMillis() > cacheExpiry;
    }

    /**
     * Mark this selection as expired
     */
    public void expire() {
        this.valid = false;
    }

    @Override
    public String toString() {
        return "TxgSelectionInfo{" +
                "serviceGroup='" + serviceGroup + '\'' +
                ", selectedTxgId='" + selectedTxgId + '\'' +
                ", endpointsCount=" + (transactionEndpoints != null ? transactionEndpoints.size() : 0) +
                ", selectionTime=" + selectionTime +
                ", valid=" + valid +
                '}';
    }
}
