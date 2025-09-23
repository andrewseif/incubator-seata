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
