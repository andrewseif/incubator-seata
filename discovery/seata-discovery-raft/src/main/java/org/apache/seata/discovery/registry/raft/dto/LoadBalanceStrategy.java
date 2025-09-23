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
