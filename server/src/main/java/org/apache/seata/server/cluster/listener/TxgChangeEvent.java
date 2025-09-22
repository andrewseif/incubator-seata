package org.apache.seata.server.cluster.listener;

import org.apache.seata.common.metadata.Node;
import org.springframework.context.ApplicationEvent;

/**
 * Event fired when TXG metadata changes
 */
public class TxgChangeEvent extends ApplicationEvent {

    private static final long serialVersionUID = 4972541203370596995L;
    private final String txgId;
    private final long term;
    private final Node newLeader;
    private final long timestamp;

    public TxgChangeEvent(Object source, String txgId, long term, Node newLeader) {
        super(source);
        this.txgId = txgId;
        this.term = term;
        this.newLeader = newLeader;
        this.timestamp = System.currentTimeMillis();
    }

    public String getTxgId() {
        return txgId;
    }

    public long getTerm() {
        return term;
    }

    public Node getNewLeader() {
        return newLeader;
    }

    public long getCurrentTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "TxgChangeEvent{" +
                "txgId='" + txgId + '\'' +
                ", term=" + term +
                ", newLeader=" + newLeader +
                ", timestamp=" + timestamp +
                '}';
    }
}
