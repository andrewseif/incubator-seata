/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seata.discovery.registry.raft.dto;

import org.apache.seata.common.metadata.Node;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

/**
 * TXG (Transaction Group) information for service discovery
 */
public class TxgInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private String txgId;
    private String serviceGroup;
    private Node leader;
    private List<Node> followers;
    private List<Node> learners;
    private long term;
    private boolean healthy;
    private long lastHealthCheck;
    private int weight;
    private Map<String, Object> metadata;

    public TxgInfo() {}

    public TxgInfo(String txgId, String serviceGroup) {
        this.txgId = txgId;
        this.serviceGroup = serviceGroup;
        this.healthy = true;
        this.weight = 1;
        this.lastHealthCheck = System.currentTimeMillis();
    }

    // Getters and Setters
    public String getTxgId() {
        return txgId;
    }

    public void setTxgId(String txgId) {
        this.txgId = txgId;
    }

    public String getServiceGroup() {
        return serviceGroup;
    }

    public void setServiceGroup(String serviceGroup) {
        this.serviceGroup = serviceGroup;
    }

    public Node getLeader() {
        return leader;
    }

    public void setLeader(Node leader) {
        this.leader = leader;
    }

    public List<Node> getFollowers() {
        return followers;
    }

    public void setFollowers(List<Node> followers) {
        this.followers = followers;
    }

    public List<Node> getLearners() {
        return learners;
    }

    public void setLearners(List<Node> learners) {
        this.learners = learners;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public boolean isHealthy() {
        return healthy;
    }

    public void setHealthy(boolean healthy) {
        this.healthy = healthy;
        this.lastHealthCheck = System.currentTimeMillis();
    }

    public long getLastHealthCheck() {
        return lastHealthCheck;
    }

    public void setLastHealthCheck(long lastHealthCheck) {
        this.lastHealthCheck = lastHealthCheck;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    /**
     * Get the transaction endpoint of the leader
     */
    public InetSocketAddress getLeaderTransactionEndpoint() {
        if (leader == null || leader.getTransaction() == null) {
            return null;
        }
        return new InetSocketAddress(leader.getTransaction().getHost(), leader.getTransaction().getPort());
    }

    /**
     * Check if this TXG is ready for transactions
     */
    public boolean isReady() {
        return healthy && leader != null && leader.getTransaction() != null;
    }

    @Override
    public String toString() {
        return "TxgInfo{" +
                "txgId='" + txgId + '\'' +
                ", serviceGroup='" + serviceGroup + '\'' +
                ", leader=" + (leader != null ? leader.getTransaction() : "null") +
                ", term=" + term +
                ", healthy=" + healthy +
                ", weight=" + weight +
                '}';
    }
}
