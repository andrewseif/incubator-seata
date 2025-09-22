/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seata.server.controller;

import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.seata.common.ConfigurationKeys;
import org.apache.seata.common.metadata.MetadataResponse;
import org.apache.seata.common.metadata.Node;
import org.apache.seata.common.result.Result;
import org.apache.seata.common.rpc.http.HttpContext;
import org.apache.seata.common.util.StringUtils;
import org.apache.seata.config.ConfigurationFactory;
import org.apache.seata.server.cluster.manager.ClusterWatcherManager;
import org.apache.seata.server.cluster.raft.RaftTransactionServer;
import org.apache.seata.server.cluster.raft.RaftTransactionServerManager;
import org.apache.seata.server.cluster.raft.RaftTransactionStateMachine;
import org.apache.seata.server.cluster.raft.service.TransactionGroupServiceManager;
import org.apache.seata.server.cluster.raft.sync.msg.dto.RaftClusterMetadata;
import org.apache.seata.server.cluster.watch.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.seata.common.ConfigurationKeys.STORE_MODE;
import static org.apache.seata.common.DefaultValues.DEFAULT_SEATA_GROUP;

@RestController
@RequestMapping("/metadata/v1")
public class ClusterController {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterController.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Resource
    private ClusterWatcherManager clusterWatcherManager;

    @Resource
    private TransactionGroupServiceManager transactionGroupServiceManager;

    @PostMapping("/changeCluster")
    public Result<?> changeCluster(@RequestParam String raftClusterStr) {
        Result<?> result = new Result<>();
        final Configuration newConf = new Configuration();
        if (!newConf.parse(raftClusterStr)) {
            result.setMessage("fail to parse initConf:" + raftClusterStr);
        } else {
            RaftTransactionServerManager.getInstance().groups().forEach(group -> {
                RaftTransactionServerManager.getInstance()
                        .getCliServiceInstance()
                        .changePeers(group, RouteTable.getInstance().getConfiguration(group), newConf);
                RouteTable.getInstance().updateConfiguration(group, newConf);
            });
        }
        return result;
    }

    @GetMapping("/cluster")
    public MetadataResponse cluster(String group) {
        MetadataResponse metadataResponse = new MetadataResponse();
        if (StringUtils.isBlank(group)) {
            group = ConfigurationFactory.getInstance()
                    .getConfig(ConfigurationKeys.SERVER_RAFT_GROUP, DEFAULT_SEATA_GROUP);
        }
        RaftTransactionServer raftTransactionServer =
                RaftTransactionServerManager.getInstance().getRaftServer(group);
        if (raftTransactionServer != null) {
            String mode = ConfigurationFactory.getInstance().getConfig(STORE_MODE);
            metadataResponse.setStoreMode(mode);
            RouteTable routeTable = RouteTable.getInstance();
            try {
                routeTable.refreshLeader(
                        RaftTransactionServerManager.getInstance().getCliClientServiceInstance(), group, 1000);
                PeerId leader = routeTable.selectLeader(group);
                if (leader != null) {
                    Set<Node> nodes = new HashSet<>();
                    RaftTransactionStateMachine raftTransactionStateMachine =
                            ((RaftTransactionStateMachine) raftTransactionServer.getRaftStateMachine());
                    RaftClusterMetadata raftClusterMetadata = raftTransactionStateMachine.getRaftLeaderMetadata();
                    Node leaderNode = raftClusterMetadata.getLeader();
                    leaderNode.setGroup(group);
                    nodes.add(leaderNode);
                    nodes.addAll(raftClusterMetadata.getLearner());
                    nodes.addAll(raftClusterMetadata.getFollowers());
                    metadataResponse.setTerm(raftClusterMetadata.getTerm());
                    metadataResponse.setNodes(new ArrayList<>(nodes));
                }
            } catch (Exception e) {
                LOGGER.error("there is an exception to getting the leader address: {}", e.getMessage(), e);
            }
        }
        return metadataResponse;
    }

    @PostMapping("/watch")
    public void watch(
            HttpContext context,
            @RequestBody Map<String, Object> groupTerms,
            @RequestParam(defaultValue = "28000") Integer timeout) {
        context.setAsync(true);
        groupTerms.forEach((group, term) -> {
            Watcher<HttpContext> watcher = new Watcher<>(group, context, timeout, Long.parseLong(String.valueOf(term)));
            clusterWatcherManager.registryWatcher(watcher);
        });
    }

    /**
     * Get all available TXGs
     */
    @GetMapping("/txgroups")
    public Result<Map<String, Object>> getTxGroups(@RequestParam(required = false) String serviceGroup) {
        Result<Map<String, Object>> result = new Result<>();

        try {
            if (!transactionGroupServiceManager.isCGMember()) {
                result.setCode("403");
                result.setMessage("Access denied: Only CG members can access TXG information");
                return result;
            }

            Map<String, Object> response = new HashMap<>();

            if (StringUtils.isNotBlank(serviceGroup)) {
                // Get TXGs for specific service group
                Map<String, List<Node>> serviceGroupTxgs = transactionGroupServiceManager.getRaftGroupsByIp("");
                List<Map<String, Object>> txgList = new ArrayList<>();

                for (Map.Entry<String, List<Node>> entry : serviceGroupTxgs.entrySet()) {
                    String txgId = entry.getKey();
                    List<Node> nodes = entry.getValue();

                    Map<String, Object> txgInfo = buildTxgResponse(txgId, nodes, serviceGroup);
                    if (txgInfo != null) {
                        txgList.add(txgInfo);
                    }
                }

                Map<String, Object> serviceGroupInfo = new HashMap<>();
                serviceGroupInfo.put("txgroups", txgList.stream().map(txg -> txg.get("txgId")).collect(Collectors.toList()));
                serviceGroupInfo.put("totalTxgs", txgList.size());
                serviceGroupInfo.put("healthyTxgs", txgList.stream().mapToLong(txg ->
                        (Boolean) txg.getOrDefault("healthy", false) ? 1 : 0).sum());

                response.put("serviceGroups", Collections.singletonMap(serviceGroup, serviceGroupInfo));

            } else {
                // Get all TXGs
                Map<String, RaftClusterMetadata> allTxgs = transactionGroupServiceManager.getAllRaftGroupsMetadata();
                List<Map<String, Object>> txgList = new ArrayList<>();

                for (Map.Entry<String, RaftClusterMetadata> entry : allTxgs.entrySet()) {
                    String txgId = entry.getKey();
                    RaftClusterMetadata metadata = entry.getValue();

                    Map<String, Object> txgInfo = buildTxgResponseFromMetadata(txgId, metadata);
                    if (txgInfo != null) {
                        txgList.add(txgInfo);
                    }
                }

                response.put("txgroups", txgList);
                response.put("totalTxgs", txgList.size());
                response.put("timestamp", System.currentTimeMillis());
            }

            result.setMessage(response.toString());
            LOGGER.debug("Retrieved TXG information: {} TXGs",
                    response.containsKey("txgroups") ? ((List<?>) response.get("txgroups")).size() : 0);

        } catch (Exception e) {
            LOGGER.error("Failed to get TXG information", e);
            result.setCode("500");
            result.setMessage("Failed to retrieve TXG information: " + e.getMessage());
        }

        return result;
    }

    /**
     * Get metadata for all TXGs or filtered TXGs
     */
    @GetMapping("/txgroups/metadata")
    public Result<Map<String, Object>> getTxgMetadata(@RequestParam(required = false) String filter) {
        Result<Map<String, Object>> result = new Result<>();

        try {
            if (!transactionGroupServiceManager.isCGMember()) {
                result.setCode("403");
                result.setMessage("Access denied: Only CG members can access TXG metadata");
                return result;
            }

            Map<String, RaftClusterMetadata> allTxgs = transactionGroupServiceManager.getAllRaftGroupsMetadata();
            Map<String, Object> response = new HashMap<>();
            List<Map<String, Object>> metadataList = new ArrayList<>();

            for (Map.Entry<String, RaftClusterMetadata> entry : allTxgs.entrySet()) {
                String txgId = entry.getKey();
                RaftClusterMetadata metadata = entry.getValue();

                // Apply filter if specified
                if (StringUtils.isNotBlank(filter) && !txgId.contains(filter)) {
                    continue;
                }

                Map<String, Object> txgMetadata = buildDetailedTxgMetadata(txgId, metadata);
                metadataList.add(txgMetadata);
            }

            response.put("txgroups", metadataList);
            response.put("totalCount", metadataList.size());
            response.put("filter", filter);
            response.put("timestamp", System.currentTimeMillis());

            result.setMessage(response.toString());
            LOGGER.debug("Retrieved TXG metadata: {} TXGs (filter: {})", metadataList.size(), filter);

        } catch (Exception e) {
            LOGGER.error("Failed to get TXG metadata", e);
            result.setCode("500");
            result.setMessage("Failed to retrieve TXG metadata: " + e.getMessage());
        }

        return result;
    }

    /**
     * Get leader information for specific TXG
     */
    @GetMapping("/txgroups/{txgId}/leader")
    public Result<Map<String, Object>> getTxgLeader(@PathVariable String txgId) {
        Result<Map<String, Object>> result = new Result<>();

        try {
            if (!transactionGroupServiceManager.isCGMember()) {
                result.setCode("403");
                result.setMessage("Access denied: Only CG members can access TXG information");
                return result;
            }

            if (StringUtils.isBlank(txgId)) {
                result.setCode("400");
                result.setMessage("TXG ID cannot be empty");
                return result;
            }

            RaftClusterMetadata txgMetadata = transactionGroupServiceManager.getTxgClusterMetadata(txgId);

            if (txgMetadata == null) {
                result.setCode("404");
                result.setMessage("TXG not found: " + txgId);
                return result;
            }

            Map<String, Object> response = new HashMap<>();
            Node leader = txgMetadata.getLeader();

            if (leader != null) {
                Map<String, Object> leaderInfo = nodeToMap(leader);
                leaderInfo.put("term", txgMetadata.getTerm());
                leaderInfo.put("role", "LEADER");

                response.put("txgId", txgId);
                response.put("leader", leaderInfo);
                response.put("hasLeader", true);
                response.put("term", txgMetadata.getTerm());

            } else {
                response.put("txgId", txgId);
                response.put("hasLeader", false);
                response.put("term", txgMetadata.getTerm());
                response.put("message", "No leader available for TXG: " + txgId);
            }

            response.put("timestamp", System.currentTimeMillis());
            result.setMessage(response.toString());

            LOGGER.debug("Retrieved leader information for TXG: {}", txgId);

        } catch (Exception e) {
            LOGGER.error("Failed to get leader for TXG: {}", txgId, e);
            result.setCode("500");
            result.setMessage("Failed to retrieve leader information: " + e.getMessage());
        }

        return result;
    }

    /**
     * Get health status for specific TXG
     */
    @GetMapping("/txgroups/{txgId}/health")
    public Result<Map<String, Object>> getTxgHealth(@PathVariable String txgId) {
        Result<Map<String, Object>> result = new Result<>();

        try {
            if (!transactionGroupServiceManager.isCGMember()) {
                result.setCode("403");
                result.setMessage("Access denied: Only CG members can access TXG information");
                return result;
            }

            RaftClusterMetadata txgMetadata = transactionGroupServiceManager.getTxgClusterMetadata(txgId);

            if (txgMetadata == null) {
                result.setCode("404");
                result.setMessage("TXG not found: " + txgId);
                return result;
            }

            Map<String, Object> response = new HashMap<>();
            response.put("txgId", txgId);
            response.put("healthy", txgMetadata.getLeader() != null);
            response.put("term", txgMetadata.getTerm());
            response.put("leaderAvailable", txgMetadata.getLeader() != null);
            response.put("followerCount", txgMetadata.getFollowers() != null ? txgMetadata.getFollowers().size() : 0);
            response.put("learnerCount", txgMetadata.getLearner() != null ? txgMetadata.getLearner().size() : 0);
            response.put("timestamp", System.currentTimeMillis());

            result.setMessage(response.toString());
            LOGGER.debug("Retrieved health status for TXG: {}", txgId);

        } catch (Exception e) {
            LOGGER.error("Failed to get health for TXG: {}", txgId, e);
            result.setCode("500");
            result.setMessage("Failed to retrieve health information: " + e.getMessage());
        }

        return result;
    }

    /**
     * Get cluster overview with all TXGs summary
     */
    @GetMapping("/txgroups/overview")
    public Result<Map<String, Object>> getTxgOverview() {
        Result<Map<String, Object>> result = new Result<>();

        try {
            if (!transactionGroupServiceManager.isCGMember()) {
                result.setCode("403");
                result.setMessage("Access denied: Only CG members can access TXG information");
                return result;
            }

            Map<String, RaftClusterMetadata> allTxgs = transactionGroupServiceManager.getAllRaftGroupsMetadata();
            Map<String, Object> response = new HashMap<>();

            int totalTxgs = allTxgs.size();
            int healthyTxgs = 0;
            int txgsWithLeader = 0;
            List<Map<String, Object>> txgSummary = new ArrayList<>();

            for (Map.Entry<String, RaftClusterMetadata> entry : allTxgs.entrySet()) {
                String txgId = entry.getKey();
                RaftClusterMetadata metadata = entry.getValue();

                boolean hasLeader = metadata.getLeader() != null;
                boolean isHealthy = hasLeader; // Simple health check

                if (isHealthy) healthyTxgs++;
                if (hasLeader) txgsWithLeader++;

                Map<String, Object> summary = new HashMap<>();
                summary.put("txgId", txgId);
                summary.put("healthy", isHealthy);
                summary.put("hasLeader", hasLeader);
                summary.put("term", metadata.getTerm());
                summary.put("memberCount",
                        (hasLeader ? 1 : 0) +
                                (metadata.getFollowers() != null ? metadata.getFollowers().size() : 0) +
                                (metadata.getLearner() != null ? metadata.getLearner().size() : 0));

                txgSummary.add(summary);
            }

            response.put("totalTxgs", totalTxgs);
            response.put("healthyTxgs", healthyTxgs);
            response.put("txgsWithLeader", txgsWithLeader);
            response.put("healthPercentage", totalTxgs > 0 ? (healthyTxgs * 100.0 / totalTxgs) : 0);
            response.put("txgSummary", txgSummary);
            response.put("timestamp", System.currentTimeMillis());

            result.setMessage(response.toString());
            LOGGER.debug("Retrieved TXG overview: {} total, {} healthy", totalTxgs, healthyTxgs);

        } catch (Exception e) {
            LOGGER.error("Failed to get TXG overview", e);
            result.setCode("500");
            result.setMessage("Failed to retrieve TXG overview: " + e.getMessage());
        }

        return result;
    }

    /**
     * Watch for TXG changes (long-polling)
     */
    @PostMapping("/watch/txgroups")
    public void watchTxGroups(
            HttpContext context,
            @RequestBody Map<String, Object> requestBody,
            @RequestParam(defaultValue = "28000") Integer timeout) {

        try {
            if (!transactionGroupServiceManager.isCGMember()) {
                sendErrorResponse(context, HttpResponseStatus.FORBIDDEN,
                        "Access denied: Only CG members can watch TXG changes");
                return;
            }

            // Extract TXG terms from request
            Map<String, Long> txgTerms = extractTxgTermsFromRequest(requestBody);

            if (txgTerms.isEmpty()) {
                sendErrorResponse(context, HttpResponseStatus.BAD_REQUEST,
                        "Invalid request: no TXG terms provided");
                return;
            }

            context.setAsync(true);

            // Create watchers for each TXG
            for (Map.Entry<String, Long> entry : txgTerms.entrySet()) {
                String txgId = entry.getKey();
                Long clientTerm = entry.getValue();

                // Check if TXG has newer term immediately
                RaftClusterMetadata currentMetadata = transactionGroupServiceManager.getTxgClusterMetadata(txgId);
                if (currentMetadata != null && currentMetadata.getTerm() > clientTerm) {
                    // Immediate response - TXG has changed
                    sendWatchResponse(context, HttpResponseStatus.OK,
                            Collections.singletonMap(txgId, currentMetadata.getTerm()));
                    return;
                }

                // Register watcher for this TXG
                Watcher<HttpContext> watcher = new Watcher<>(txgId, context, timeout, clientTerm);
                clusterWatcherManager.registryTxgWatcher(watcher);
            }

            LOGGER.debug("Registered TXG watchers for {} TXGs with timeout {}ms", txgTerms.size(), timeout);

        } catch (Exception e) {
            LOGGER.error("Failed to setup TXG watch", e);
            sendErrorResponse(context, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    "Failed to setup TXG watch: " + e.getMessage());
        }
    }

    /**
     * Watch for changes to specific TXG
     */
    @PostMapping("/watch/txgroups/{txgId}")
    public void watchSpecificTxg(
            HttpContext context,
            @PathVariable String txgId,
            @RequestBody Map<String, Object> requestBody,
            @RequestParam(defaultValue = "28000") Integer timeout) {

        try {
            if (!transactionGroupServiceManager.isCGMember()) {
                sendErrorResponse(context, HttpResponseStatus.FORBIDDEN,
                        "Access denied: Only CG members can watch TXG changes");
                return;
            }

            if (StringUtils.isBlank(txgId)) {
                sendErrorResponse(context, HttpResponseStatus.BAD_REQUEST, "TXG ID cannot be empty");
                return;
            }

            // Extract client's current term
            Long clientTerm = extractClientTerm(requestBody);

            // Check current TXG metadata
            RaftClusterMetadata currentMetadata = transactionGroupServiceManager.getTxgClusterMetadata(txgId);

            if (currentMetadata == null) {
                sendErrorResponse(context, HttpResponseStatus.NOT_FOUND, "TXG not found: " + txgId);
                return;
            }

            if (currentMetadata.getTerm() > clientTerm) {
                // Immediate response - TXG has changed
                Map<String, Object> response = new HashMap<>();
                response.put("txgId", txgId);
                response.put("currentTerm", currentMetadata.getTerm());
                response.put("changed", true);

                sendWatchResponse(context, HttpResponseStatus.OK, response);
                return;
            }

            context.setAsync(true);

            // Register watcher for this specific TXG
            Watcher<HttpContext> watcher = new Watcher<>(txgId, context, timeout, clientTerm);
            clusterWatcherManager.registryTxgWatcher(watcher);

            LOGGER.debug("Registered watcher for TXG {} (term: {}, timeout: {}ms)", txgId, clientTerm, timeout);

        } catch (Exception e) {
            LOGGER.error("Failed to setup watch for TXG: {}", txgId, e);
            sendErrorResponse(context, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    "Failed to setup TXG watch: " + e.getMessage());
        }
    }

    /**
     * Get current status of all watched TXGs
     */
    @GetMapping("/watch/txgroups/status")
    public Result<Map<String, Object>> getWatchStatus() {
        Result<Map<String, Object>> result = new Result<>();

        try {
            if (!transactionGroupServiceManager.isCGMember()) {
                result.setCode("403");
                result.setMessage("Access denied: Only CG members can access watch status");
                return result;
            }

            Map<String, Object> response = new HashMap<>();

            // Get current terms for all TXGs
            Map<String, RaftClusterMetadata> allTxgs = transactionGroupServiceManager.getAllRaftGroupsMetadata();
            Map<String, Long> currentTerms = new HashMap<>();

            for (Map.Entry<String, RaftClusterMetadata> entry : allTxgs.entrySet()) {
                currentTerms.put(entry.getKey(), entry.getValue().getTerm());
            }

            response.put("currentTerms", currentTerms);
            response.put("totalTxgs", allTxgs.size());
            response.put("timestamp", System.currentTimeMillis());

            // Add watch statistics if available
            response.put("activeWatchers", clusterWatcherManager.getActiveWatcherCount());

            result.setMessage(response.toString());

        } catch (Exception e) {
            LOGGER.error("Failed to get watch status", e);
            result.setCode("500");
            result.setMessage("Failed to retrieve watch status: " + e.getMessage());
        }

        return result;
    }

// Helper methods for watch functionality

    @SuppressWarnings("unchecked")
    private Map<String, Long> extractTxgTermsFromRequest(Map<String, Object> requestBody) {
        Map<String, Long> txgTerms = new HashMap<>();

        if (requestBody.containsKey("txgTerms")) {
            Object txgTermsObj = requestBody.get("txgTerms");
            if (txgTermsObj instanceof Map) {
                Map<String, Object> terms = (Map<String, Object>) txgTermsObj;
                for (Map.Entry<String, Object> entry : terms.entrySet()) {
                    try {
                        Long term = Long.valueOf(entry.getValue().toString());
                        txgTerms.put(entry.getKey(), term);
                    } catch (NumberFormatException e) {
                        LOGGER.warn("Invalid term value for TXG {}: {}", entry.getKey(), entry.getValue());
                    }
                }
            }
        }

        return txgTerms;
    }

    private Long extractClientTerm(Map<String, Object> requestBody) {
        if (requestBody.containsKey("currentTerm")) {
            try {
                return Long.valueOf(requestBody.get("currentTerm").toString());
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid current term value: {}", requestBody.get("currentTerm"));
            }
        }
        return 0L; // Default to 0 if not provided
    }

    /**
     * Send error response for watch requests (corrected version)
     */
    private void sendErrorResponse(HttpContext context, HttpResponseStatus status, String message) {
        try {
            ChannelHandlerContext ctx = context.getContext();
            if (!context.isHttp2() && ctx.channel().isActive()) {

                HttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, Unpooled.EMPTY_BUFFER);
                response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);

                if (StringUtils.isNotBlank(message)) {
                    response.headers().set("X-Error-Message", message);
                }

                if (!context.isKeepAlive()) {
                    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
                } else {
                    ctx.writeAndFlush(response);
                }

                LOGGER.debug("Sent watch error response: {} - {}", status, message);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to send watch error response", e);
        }
    }

    /**
     * Send watch response (corrected version)
     */
    private void sendWatchResponse(HttpContext context, HttpResponseStatus status, Object responseData) {
        try {
            ChannelHandlerContext ctx = context.getContext();
            if (!context.isHttp2() && ctx.channel().isActive()) {
                HttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, Unpooled.EMPTY_BUFFER);
                response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);

                if (responseData instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> data = (Map<String, Object>) responseData;

                    if (data.containsKey("txgId")) {
                        response.headers().set("X-TXG-ID", data.get("txgId").toString());
                    }
                    if (data.containsKey("newTerm")) {
                        response.headers().set("X-New-Term", data.get("newTerm").toString());
                    }
                }

                if (!context.isKeepAlive()) {
                    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
                } else {
                    ctx.writeAndFlush(response);
                }

                LOGGER.debug("Sent watch response: {}", status);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to send watch response", e);
        }
    }

// Helper methods for building responses

    private Map<String, Object> buildTxgResponse(String txgId, List<Node> nodes, String serviceGroup) {
        if (CollectionUtils.isEmpty(nodes)) {
            return null;
        }

        Map<String, Object> txgInfo = new HashMap<>();
        txgInfo.put("txgId", txgId);
        txgInfo.put("serviceGroup", serviceGroup);

        // Find leader (simplified - assumes first node is leader)
        Node leader = nodes.stream().findFirst().orElse(null);
        if (leader != null) {
            txgInfo.put("leader", nodeToMap(leader));
            txgInfo.put("healthy", true);
        } else {
            txgInfo.put("healthy", false);
        }

        txgInfo.put("memberCount", nodes.size());
        txgInfo.put("timestamp", System.currentTimeMillis());

        return txgInfo;
    }

    private Map<String, Object> buildTxgResponseFromMetadata(String txgId, RaftClusterMetadata metadata) {
        Map<String, Object> txgInfo = new HashMap<>();
        txgInfo.put("txgId", txgId);
        txgInfo.put("term", metadata.getTerm());

        if (metadata.getLeader() != null) {
            txgInfo.put("leader", nodeToMap(metadata.getLeader()));
            txgInfo.put("healthy", true);
        } else {
            txgInfo.put("healthy", false);
        }

        List<Map<String, Object>> followers = new ArrayList<>();
        if (metadata.getFollowers() != null) {
            for (Node follower : metadata.getFollowers()) {
                followers.add(nodeToMap(follower));
            }
        }
        txgInfo.put("followers", followers);

        List<Map<String, Object>> learners = new ArrayList<>();
        if (metadata.getLearner() != null) {
            for (Node learner : metadata.getLearner()) {
                learners.add(nodeToMap(learner));
            }
        }
        txgInfo.put("learners", learners);

        txgInfo.put("memberCount",
                (metadata.getLeader() != null ? 1 : 0) +
                        followers.size() +
                        learners.size());

        return txgInfo;
    }

    private Map<String, Object> buildDetailedTxgMetadata(String txgId, RaftClusterMetadata metadata) {
        Map<String, Object> detailed = buildTxgResponseFromMetadata(txgId, metadata);

        // Add additional metadata
        detailed.put("clusterSize", detailed.get("memberCount"));
        detailed.put("leaderAvailable", metadata.getLeader() != null);
        detailed.put("followerCount", metadata.getFollowers() != null ? metadata.getFollowers().size() : 0);
        detailed.put("learnerCount", metadata.getLearner() != null ? metadata.getLearner().size() : 0);

        // Add endpoints summary
        List<String> endpoints = new ArrayList<>();
        if (metadata.getLeader() != null && metadata.getLeader().getTransaction() != null) {
            endpoints.add(metadata.getLeader().getTransaction().getHost() + ":" +
                    metadata.getLeader().getTransaction().getPort());
        }
        detailed.put("transactionEndpoints", endpoints);

        return detailed;
    }

    private Map<String, Object> nodeToMap(Node node) {
        Map<String, Object> nodeMap = new HashMap<>();

        if (node.getTransaction() != null) {
            nodeMap.put("transactionHost", node.getTransaction().getHost());
            nodeMap.put("transactionPort", node.getTransaction().getPort());
        }

        if (node.getControl() != null) {
            nodeMap.put("controlHost", node.getControl().getHost());
            nodeMap.put("controlPort", node.getControl().getPort());
        }

        if (node.getInternal() != null) {
            nodeMap.put("raftHost", node.getInternal().getHost());
            nodeMap.put("raftPort", node.getInternal().getPort());
        }

        nodeMap.put("group", node.getGroup());
        nodeMap.put("version", node.getVersion());

        if (node.getMetadata() != null) {
            nodeMap.put("metadata", node.getMetadata());
        }

        return nodeMap;
    }

    /**
     * Get comprehensive monitoring information for all TXG operations
     */
    @GetMapping("/txgroups/monitor")
    public Result<Map<String, Object>> getTxgMonitoring() {
        Result<Map<String, Object>> result = new Result<>();

        try {
            if (!transactionGroupServiceManager.isCGMember()) {
                result.setCode("403");
                result.setMessage("Access denied: Only CG members can access monitoring information");
                return result;
            }

            Map<String, Object> monitoring = new HashMap<>();

            // TXG statistics
            Map<String, RaftClusterMetadata> allTxgs = transactionGroupServiceManager.getAllRaftGroupsMetadata();
            monitoring.put("txgStatistics", buildTxgStatistics(allTxgs));

            // Watcher statistics
            monitoring.put("watcherStatistics", clusterWatcherManager.getTxgWatcherStatistics());

            // System health
            monitoring.put("systemHealth", buildSystemHealth());

            // Recent events (if available)
            monitoring.put("recentEvents", getRecentTxgEvents());

            result.setMessage(monitoring.toString());

        } catch (Exception e) {
            LOGGER.error("Failed to get TXG monitoring information", e);
            result.setCode("500");
            result.setMessage("Failed to retrieve monitoring information: " + e.getMessage());
        }

        return result;
    }

    private Map<String, Object> buildTxgStatistics(Map<String, RaftClusterMetadata> allTxgs) {
        Map<String, Object> stats = new HashMap<>();

        int totalTxgs = allTxgs.size();
        int healthyTxgs = 0;
        int txgsWithLeader = 0;
        long totalMembers = 0;

        for (RaftClusterMetadata metadata : allTxgs.values()) {
            boolean hasLeader = metadata.getLeader() != null;
            if (hasLeader) {
                txgsWithLeader++;
                healthyTxgs++; // Simple health check
            }

            totalMembers += (hasLeader ? 1 : 0) +
                    (metadata.getFollowers() != null ? metadata.getFollowers().size() : 0) +
                    (metadata.getLearner() != null ? metadata.getLearner().size() : 0);
        }

        stats.put("totalTxgs", totalTxgs);
        stats.put("healthyTxgs", healthyTxgs);
        stats.put("txgsWithLeader", txgsWithLeader);
        stats.put("totalMembers", totalMembers);
        stats.put("averageClusterSize", totalTxgs > 0 ? (double) totalMembers / totalTxgs : 0);
        stats.put("healthPercentage", totalTxgs > 0 ? (double) healthyTxgs * 100 / totalTxgs : 100);

        return stats;
    }

    private Map<String, Object> buildSystemHealth() {
        Map<String, Object> health = new HashMap<>();

        try {
            // JVM health
            Runtime runtime = Runtime.getRuntime();
            health.put("jvmMemoryUsed", runtime.totalMemory() - runtime.freeMemory());
            health.put("jvmMemoryFree", runtime.freeMemory());
            health.put("jvmMemoryTotal", runtime.totalMemory());
            health.put("jvmMemoryMax", runtime.maxMemory());

            // Thread health
            health.put("activeThreads", Thread.activeCount());

            // System load (simplified)
            health.put("systemStatus", "healthy"); // Could be enhanced with actual health checks

        } catch (Exception e) {
            health.put("systemStatus", "error");
            health.put("error", e.getMessage());
        }

        return health;
    }

    private List<Map<String, Object>> getRecentTxgEvents() {
        // Placeholder for recent events tracking, we might need to track recent TXG change events
        List<Map<String, Object>> events = new ArrayList<>();

        Map<String, Object> sampleEvent = new HashMap<>();
        sampleEvent.put("type", "TXG_LEADER_CHANGE");
        sampleEvent.put("timestamp", System.currentTimeMillis());
        sampleEvent.put("message", "Recent events tracking not implemented");
        events.add(sampleEvent);

        return events;
    }

    /**
     * Receive TXG metadata updates from TXG nodes
     */
    @PostMapping("/metadata/v1/txgroups/update")
    public Result<Map<String, Object>> updateTxgMetadata(@RequestBody Map<String, Object> updateRequest) {
        Result<Map<String, Object>> result = new Result<>();

        try {
            if (!transactionGroupServiceManager.isCGMember()) {
                result.setCode("403");
                result.setMessage("Access denied: Only CG members can receive TXG updates");
                return result;
            }

            // Validate request
            if (!updateRequest.containsKey("txgId") || !updateRequest.containsKey("term")) {
                result.setCode("400");
                result.setMessage("Invalid request: missing txgId or term");
                return result;
            }

            String txgId = updateRequest.get("txgId").toString();
            long newTerm = Long.parseLong(updateRequest.get("term").toString());

            LOGGER.info("Received TXG metadata update: {} (term: {})", txgId, newTerm);

            // Parse the updated metadata
            RaftClusterMetadata updatedMetadata = parseMetadataFromUpdateRequest(updateRequest);
            updatedMetadata.setTerm(newTerm);

            // Validate that this is a newer update
            RaftClusterMetadata currentMetadata = transactionGroupServiceManager.getTxgClusterMetadata(txgId);

            if (currentMetadata != null && newTerm <= currentMetadata.getTerm()) {
                LOGGER.debug("Ignoring stale TXG metadata update: {} (current: {}, received: {})",
                        txgId, currentMetadata.getTerm(), newTerm);

                result.setCode("409");
                result.setMessage("Stale update ignored");
                return result;
            }

            // Update the metadata
            transactionGroupServiceManager.updateTxgClusterMetadata(txgId, updatedMetadata);

            // Notify watchers of the change
            clusterWatcherManager.notifyTxgWatchers(txgId, newTerm);

            // Prepare response
            Map<String, Object> response = new HashMap<>();
            response.put("txgId", txgId);
            response.put("term", newTerm);
            response.put("updated", true);
            response.put("timestamp", System.currentTimeMillis());

            result.setMessage(response.toString());

            LOGGER.info("Successfully processed TXG metadata update: {} (term: {})", txgId, newTerm);

        } catch (NumberFormatException e) {
            LOGGER.error("Invalid term value in TXG update request", e);
            result.setCode("400");
            result.setMessage("Invalid term value");
        } catch (Exception e) {
            LOGGER.error("Failed to process TXG metadata update", e);
            result.setCode("500");
            result.setMessage("Failed to process update: " + e.getMessage());
        }

        return result;
    }

    /**
     * Parse metadata from update request
     */
    private RaftClusterMetadata parseMetadataFromUpdateRequest(Map<String, Object> updateRequest) {
        RaftClusterMetadata metadata = new RaftClusterMetadata();

        // Parse leader
        if (updateRequest.containsKey("leader")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> leaderMap = (Map<String, Object>) updateRequest.get("leader");
            Node leader = parseNodeFromMap(leaderMap);
            metadata.setLeader(leader);
        }

        // Parse followers
        if (updateRequest.containsKey("followers")) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> followersData = (List<Map<String, Object>>) updateRequest.get("followers");
            List<Node> followers = followersData.stream()
                    .map(this::parseNodeFromMap)
                    .collect(Collectors.toList());
            metadata.setFollowers(followers);
        }

        // Parse learners
        if (updateRequest.containsKey("learners")) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> learnersData = (List<Map<String, Object>>) updateRequest.get("learners");
            List<Node> learners = learnersData.stream()
                    .map(this::parseNodeFromMap)
                    .collect(Collectors.toList());
            metadata.setLearner(learners);
        }

        return metadata;
    }

    /**
     * Parse Node from map data
     */
    private Node parseNodeFromMap(Map<String, Object> nodeMap) {
        Node node = new Node();

        // Parse transaction endpoint
        if (nodeMap.containsKey("transaction")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> transactionMap = (Map<String, Object>) nodeMap.get("transaction");
            String host = transactionMap.get("host").toString();
            int port = Integer.parseInt(transactionMap.get("port").toString());
            node.setTransaction(node.createEndpoint(host, port, "transaction"));
        }

        // Parse control endpoint
        if (nodeMap.containsKey("control")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> controlMap = (Map<String, Object>) nodeMap.get("control");
            String host = controlMap.get("host").toString();
            int port = Integer.parseInt(controlMap.get("port").toString());
            node.setControl(node.createEndpoint(host, port, "control"));
        }

        // Parse internal endpoint
        if (nodeMap.containsKey("internal")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> internalMap = (Map<String, Object>) nodeMap.get("internal");
            String host = internalMap.get("host").toString();
            int port = Integer.parseInt(internalMap.get("port").toString());
            node.setInternal(node.createEndpoint(host, port, "raft"));
        }

        // Parse other fields
        if (nodeMap.containsKey("group")) {
            node.setGroup(nodeMap.get("group").toString());
        }

        if (nodeMap.containsKey("version")) {
            node.setVersion(nodeMap.get("version").toString());
        }

        if (nodeMap.containsKey("metadata")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> metadata = (Map<String, Object>) nodeMap.get("metadata");
            node.setMetadata(metadata);
        }

        return node;
    }

}
