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
package org.apache.seata.discovery.registry.raft;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.apache.seata.common.ConfigurationKeys;
import org.apache.seata.common.exception.AuthenticationFailedException;
import org.apache.seata.common.exception.NotSupportYetException;
import org.apache.seata.common.exception.ParseEndpointException;
import org.apache.seata.common.exception.RetryableException;
import org.apache.seata.common.metadata.Metadata;
import org.apache.seata.common.metadata.MetadataResponse;
import org.apache.seata.common.metadata.Node;
import org.apache.seata.common.thread.NamedThreadFactory;
import org.apache.seata.common.util.CollectionUtils;
import org.apache.seata.common.util.HttpClientUtil;
import org.apache.seata.common.util.NetUtil;
import org.apache.seata.common.util.StringUtils;
import org.apache.seata.config.ConfigChangeListener;
import org.apache.seata.config.Configuration;
import org.apache.seata.config.ConfigurationFactory;
import org.apache.seata.discovery.registry.RegistryService;
import org.apache.seata.discovery.registry.raft.dto.LoadBalanceStrategy;
import org.apache.seata.discovery.registry.raft.dto.TxgInfo;
import org.apache.seata.discovery.registry.raft.dto.TxgSelectionInfo;
import org.apache.seata.server.cluster.raft.sync.msg.dto.RaftClusterMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The type File registry service.
 *
 */
public class RaftRegistryServiceImpl implements RegistryService<ConfigChangeListener> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftRegistryServiceImpl.class);

    private static final String REGISTRY_TYPE = "raft";

    private static final String PRO_SERVER_ADDR_KEY = "serverAddr";

    private static final String PRO_USERNAME_KEY = "username";

    private static final String PRO_PASSWORD_KEY = "password";

    private static final String AUTHORIZATION_HEADER = "Authorization";

    private static final String TOKEN_VALID_TIME_MS_KEY = "tokenValidityInMilliseconds";

    private static final String META_DATA_MAX_AGE_MS = "metadataMaxAgeMs";

    private static final long TOKEN_EXPIRE_TIME_IN_MILLISECONDS;

    private static final String USERNAME;

    private static final String PASSWORD;

    public static String jwtToken;

    private static long tokenTimeStamp = -1;

    private static volatile RaftRegistryServiceImpl instance;

    private static final Configuration CONFIG = ConfigurationFactory.CURRENT_FILE_INSTANCE;

    private static final String IP_PORT_SPLIT_CHAR = ":";

    private static final Map<String, List<InetSocketAddress>> INIT_ADDRESSES = new HashMap<>();

    private static final Metadata METADATA = new Metadata();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static volatile String CURRENT_TRANSACTION_SERVICE_GROUP;

    private static volatile String CURRENT_TRANSACTION_CLUSTER_NAME;

    private static volatile ThreadPoolExecutor REFRESH_METADATA_EXECUTOR;

    private static final AtomicBoolean CLOSED = new AtomicBoolean(false);

    // CG (Controller Group) related configuration keys
    private static final String PRO_CG_SERVER_ADDR_KEY = "cgServerAddr";
    private static final String PRO_TXG_LOAD_BALANCE_STRATEGY_KEY = "txgLoadBalanceStrategy";
    private static final String PRO_TXG_SELECTION_CACHE_TIME_KEY = "txgSelectionCacheTime";
    private static final String PRO_CG_DISCOVERY_INTERVAL_KEY = "cgDiscoveryInterval";
    private static final String PRO_TXG_HEALTH_CHECK_INTERVAL_KEY = "txgHealthCheckInterval";

    // Default values for CG configuration
    private static final String DEFAULT_TXG_LOAD_BALANCE_STRATEGY = "round_robin";
    private static final long DEFAULT_TXG_SELECTION_CACHE_TIME = 60000L; // 1 minute
    private static final long DEFAULT_CG_DISCOVERY_INTERVAL = 30000L; // 30 seconds
    private static final long DEFAULT_TXG_HEALTH_CHECK_INTERVAL = 10000L; // 10 seconds

    /**
     * Cache for all available TXGs discovered from CG
     * Key: TXG ID, Value: TXG information
     */
    private static final Map<String, TxgInfo> TXG_CACHE = new ConcurrentHashMap<>();

    /**
     * Cache for TXG selections per service group
     * Key: Service Group, Value: Selected TXG information
     */
    private static final Map<String, TxgSelectionInfo> TXG_SELECTION_CACHE = new ConcurrentHashMap<>();

    /**
     * Cache for service group to available TXG IDs mapping
     * Key: Service Group, Value: List of TXG IDs that can serve this service group
     */
    private static final Map<String, List<String>> SERVICE_GROUP_TXG_MAPPING = new ConcurrentHashMap<>();

    /**
     * Last update time for TXG cache
     */
    private static volatile long TXG_CACHE_LAST_UPDATE = 0L;

    /**
     * Round robin counters for load balancing
     * Key: Service Group, Value: Counter for round robin selection
     */
    private static final Map<String, AtomicInteger> ROUND_ROBIN_COUNTERS = new ConcurrentHashMap<>();

    /**
     * TXG health status tracking
     * Key: TXG ID, Value: Last successful health check timestamp
     */
    private static final Map<String, Long> TXG_HEALTH_STATUS = new ConcurrentHashMap<>();

    /**
     * Executor for TXG-related background tasks
     */
    private static volatile ThreadPoolExecutor TXG_MANAGEMENT_EXECUTOR;

    /**
     * Flag to indicate if TXG cache initialization is in progress
     */
    private static final AtomicBoolean TXG_CACHE_INITIALIZING = new AtomicBoolean(false);

    /**
     * Service node health check
     */
    private static final Map<String, List<InetSocketAddress>> ALIVE_NODES = new ConcurrentHashMap<>();

    private static final String PREFERRED_NETWORKS;
    private static final String CG_SERVER_ADDRESSES;
    private static final String TXG_LOAD_BALANCE_STRATEGY;
    private static final long TXG_SELECTION_CACHE_TIME;
    private static final long CG_DISCOVERY_INTERVAL;
    private static final long TXG_HEALTH_CHECK_INTERVAL;
    private static final Map<String, List<InetSocketAddress>> CG_ADDRESSES = new HashMap<>();
    private static final AtomicBoolean CG_MODE_ENABLED = new AtomicBoolean(false);

    static {
        TOKEN_EXPIRE_TIME_IN_MILLISECONDS = CONFIG.getLong(getTokenExpireTimeInMillisecondsKey(), 29 * 60 * 1000L);
        USERNAME = CONFIG.getConfig(getRaftUserNameKey());
        PASSWORD = CONFIG.getConfig(getRaftPassWordKey());
        PREFERRED_NETWORKS = CONFIG.getConfig(getPreferredNetworks());

        // CG (Controller Group) configuration initialization
        CG_SERVER_ADDRESSES = CONFIG.getConfig(getCgAddrFileKey());
        TXG_LOAD_BALANCE_STRATEGY = CONFIG.getConfig(getTxgLoadBalanceStrategyKey(), DEFAULT_TXG_LOAD_BALANCE_STRATEGY);
        TXG_SELECTION_CACHE_TIME = CONFIG.getLong(getTxgSelectionCacheTimeKey(), DEFAULT_TXG_SELECTION_CACHE_TIME);
        CG_DISCOVERY_INTERVAL = CONFIG.getLong(getCgDiscoveryIntervalKey(), DEFAULT_CG_DISCOVERY_INTERVAL);
        TXG_HEALTH_CHECK_INTERVAL = CONFIG.getLong(getTxgHealthCheckIntervalKey(), DEFAULT_TXG_HEALTH_CHECK_INTERVAL);

        // Initialize CG mode
        CG_MODE_ENABLED.set(initializeCgAddresses());
        validateCgConfiguration();

        // Initialize TXG cache management
        initializeTxgCacheManagement();

        // Log final configuration
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Registry mode: {}, CG addresses: {}, TXG management: {}",
                    isCgModeEnabled() ? "CG-based multi-raft" : "Traditional single-raft",
                    isCgModeEnabled() ? getCgAddresses().size() : "N/A",
                    isCgModeEnabled() ? "enabled" : "disabled");
        }

        // Log CG configuration for debugging
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("CG configuration loaded - addresses: {}, loadBalanceStrategy: {}, cacheTime: {}ms, discoveryInterval: {}ms",
                    CG_SERVER_ADDRESSES, TXG_LOAD_BALANCE_STRATEGY, TXG_SELECTION_CACHE_TIME, CG_DISCOVERY_INTERVAL);
        }
    }


    private RaftRegistryServiceImpl() {}

    /**
     * Gets instance.
     *
     * @return the instance
     */
    static RaftRegistryServiceImpl getInstance() {
        if (instance == null) {
            synchronized (RaftRegistryServiceImpl.class) {
                if (instance == null) {
                    instance = new RaftRegistryServiceImpl();
                }
            }
        }
        return instance;
    }

    @Override
    public void register(InetSocketAddress address) throws Exception {}

    @Override
    public void unregister(InetSocketAddress address) throws Exception {}

    @Override
    public void subscribe(String cluster, ConfigChangeListener listener) throws Exception {}

    @Override
    public void unsubscribe(String cluster, ConfigChangeListener listener) throws Exception {}

    protected static void startQueryMetadata() {
        if (REFRESH_METADATA_EXECUTOR == null) {
            synchronized (INIT_ADDRESSES) {
                if (REFRESH_METADATA_EXECUTOR == null) {
                    REFRESH_METADATA_EXECUTOR = new ThreadPoolExecutor(
                            1,
                            1,
                            0L,
                            TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue<>(),
                            new NamedThreadFactory("refreshMetadata", 1, true));
                    REFRESH_METADATA_EXECUTOR.execute(() -> {
                        long metadataMaxAgeMs = CONFIG.getLong(getMetadataMaxAgeMs(), 30000L);
                        long currentTime = System.currentTimeMillis();
                        while (!CLOSED.get()) {
                            try {
                                // Forced refresh of metadata information after set age
                                boolean fetch = System.currentTimeMillis() - currentTime > metadataMaxAgeMs;
                                String clusterName = CURRENT_TRANSACTION_CLUSTER_NAME;
                                if (!fetch) {
                                    fetch = watchTraditionalCluster();
                                }
                                // Cluster changes or reaches timeout refresh time
                                if (fetch) {
                                    for (String group : METADATA.groups(clusterName)) {
                                        try {
                                            acquireClusterMetaData(clusterName, group);
                                        } catch (Exception e) {
                                            // prevents an exception from being thrown that causes the thread to break
                                            if (e instanceof RetryableException) {
                                                throw e;
                                            } else {
                                                LOGGER.error(
                                                        "failed to get the leader address,error: {}", e.getMessage());
                                            }
                                        }
                                    }
                                    currentTime = System.currentTimeMillis();
                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.debug("refresh seata cluster metadata time: {}", currentTime);
                                    }
                                }
                            } catch (RetryableException e) {
                                LOGGER.error(e.getMessage(), e);
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException ignored) {
                                }
                            }
                        }
                    });
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        CLOSED.compareAndSet(false, true);
                        REFRESH_METADATA_EXECUTOR.shutdown();
                    }));
                }
            }
        }
    }

    private static String queryHttpAddress(String clusterName, String group) {
        List<Node> nodeList = METADATA.getNodes(clusterName, group);
        List<String> addressList = null;
        Stream<InetSocketAddress> stream = null;
        if (CollectionUtils.isNotEmpty(nodeList)) {
            List<InetSocketAddress> inetSocketAddresses = ALIVE_NODES.get(CURRENT_TRANSACTION_SERVICE_GROUP);
            if (CollectionUtils.isEmpty(inetSocketAddresses)) {
                addressList = nodeList.stream()
                        .map(RaftRegistryServiceImpl::selectControlEndpointStr)
                        .collect(Collectors.toList());
            } else {
                stream = inetSocketAddresses.stream();
            }
        } else {
            stream = INIT_ADDRESSES.get(clusterName).stream();
        }
        if (addressList != null) {
            return addressList.get(ThreadLocalRandom.current().nextInt(addressList.size()));
        } else {
            Map<String, Node> map = new HashMap<>();
            if (CollectionUtils.isNotEmpty(nodeList)) {
                for (Node node : nodeList) {
                    InetSocketAddress inetSocketAddress = selectTransactionEndpoint(node);
                    map.put(inetSocketAddress.getHostString() + IP_PORT_SPLIT_CHAR + inetSocketAddress.getPort(), node);
                }
            }
            addressList = stream.map(inetSocketAddress -> {
                        String host = NetUtil.toStringHost(inetSocketAddress);
                        Node node = map.get(host + IP_PORT_SPLIT_CHAR + inetSocketAddress.getPort());
                        InetSocketAddress controlEndpoint = null;
                        if (node != null) {
                            controlEndpoint = selectControlEndpoint(node);
                        }
                        return host
                                + IP_PORT_SPLIT_CHAR
                                + (controlEndpoint != null ? controlEndpoint.getPort() : inetSocketAddress.getPort());
                    })
                    .collect(Collectors.toList());
            return addressList.get(ThreadLocalRandom.current().nextInt(addressList.size()));
        }
    }

    private static String getRaftAddrFileKey() {
        return String.join(
                ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR,
                ConfigurationKeys.FILE_ROOT_REGISTRY,
                REGISTRY_TYPE,
                PRO_SERVER_ADDR_KEY);
    }

    private static String getRaftUserNameKey() {
        return String.join(
                ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR,
                ConfigurationKeys.FILE_ROOT_REGISTRY,
                REGISTRY_TYPE,
                PRO_USERNAME_KEY);
    }

    private static String getRaftPassWordKey() {
        return String.join(
                ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR,
                ConfigurationKeys.FILE_ROOT_REGISTRY,
                REGISTRY_TYPE,
                PRO_PASSWORD_KEY);
    }

    private static String getPreferredNetworks() {
        return String.join(
                ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR, ConfigurationKeys.FILE_ROOT_REGISTRY, "preferredNetworks");
    }

    private static String getTokenExpireTimeInMillisecondsKey() {
        return String.join(
                ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR,
                ConfigurationKeys.FILE_ROOT_REGISTRY,
                REGISTRY_TYPE,
                TOKEN_VALID_TIME_MS_KEY);
    }

    private static String getCgAddrFileKey() {
        return String.join(
                ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR,
                ConfigurationKeys.FILE_ROOT_REGISTRY,
                REGISTRY_TYPE,
                PRO_CG_SERVER_ADDR_KEY);
    }

    private static String getTxgLoadBalanceStrategyKey() {
        return String.join(
                ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR,
                ConfigurationKeys.FILE_ROOT_REGISTRY,
                REGISTRY_TYPE,
                PRO_TXG_LOAD_BALANCE_STRATEGY_KEY);
    }

    private static String getTxgSelectionCacheTimeKey() {
        return String.join(
                ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR,
                ConfigurationKeys.FILE_ROOT_REGISTRY,
                REGISTRY_TYPE,
                PRO_TXG_SELECTION_CACHE_TIME_KEY);
    }

    private static String getCgDiscoveryIntervalKey() {
        return String.join(
                ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR,
                ConfigurationKeys.FILE_ROOT_REGISTRY,
                REGISTRY_TYPE,
                PRO_CG_DISCOVERY_INTERVAL_KEY);
    }

    private static String getTxgHealthCheckIntervalKey() {
        return String.join(
                ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR,
                ConfigurationKeys.FILE_ROOT_REGISTRY,
                REGISTRY_TYPE,
                PRO_TXG_HEALTH_CHECK_INTERVAL_KEY);
    }

    private static boolean isTokenExpired() {
        if (tokenTimeStamp == -1) {
            return true;
        }
        long tokenExpiredTime = tokenTimeStamp + TOKEN_EXPIRE_TIME_IN_MILLISECONDS;
        return System.currentTimeMillis() >= tokenExpiredTime;
    }

    private static String selectControlEndpointStr(Node node) {
        InetSocketAddress control = selectControlEndpoint(node);
        return NetUtil.toStringAddress(control);
    }

    private static String selectTransactionEndpointStr(Node node) {
        InetSocketAddress transaction = selectTransactionEndpoint(node);
        return NetUtil.toStringAddress(transaction);
    }

    private static InetSocketAddress selectControlEndpoint(Node node) {
        return selectEndpoint("control", node);
    }

    private static InetSocketAddress selectTransactionEndpoint(Node node) {
        return selectEndpoint("transaction", node);
    }

    private static InetSocketAddress selectEndpoint(String type, Node node) {
        if (StringUtils.isBlank(PREFERRED_NETWORKS)) {
            // Use the default method, directly using node.control and node.transaction
            switch (type) {
                case "control":
                    return new InetSocketAddress(
                            node.getControl().getHost(), node.getControl().getPort());
                case "transaction":
                    return new InetSocketAddress(
                            node.getTransaction().getHost(),
                            node.getTransaction().getPort());
                default:
                    throw new NotSupportYetException("SelectEndpoint is not support type: " + type);
            }
        }
        Node.ExternalEndpoint externalEndpoint = selectExternalEndpoint(node, PREFERRED_NETWORKS.split(";"));
        switch (type) {
            case "control":
                return new InetSocketAddress(externalEndpoint.getHost(), externalEndpoint.getControlPort());
            case "transaction":
                return new InetSocketAddress(externalEndpoint.getHost(), externalEndpoint.getTransactionPort());
            default:
                throw new NotSupportYetException("SelectEndpoint is not support type: " + type);
        }
    }

    private static Node.ExternalEndpoint selectExternalEndpoint(Node node, String[] preferredNetworks) {
        Map<String, Object> metadata = node.getMetadata();
        if (CollectionUtils.isEmpty(metadata)) {
            throw new ParseEndpointException("Node metadata is empty.");
        }

        Object external = metadata.get("external");

        if (external instanceof List<?>) {
            List<LinkedHashMap<String, Object>> externalEndpoints = (List<LinkedHashMap<String, Object>>) external;

            if (CollectionUtils.isEmpty(externalEndpoints)) {
                throw new ParseEndpointException("ExternalEndpoints should not be empty.");
            }

            for (LinkedHashMap<String, Object> externalEndpoint : externalEndpoints) {
                String ip = Optional.ofNullable(externalEndpoint.get("host"))
                        .map(Object::toString)
                        .orElse("");

                if (isPreferredNetwork(ip, Arrays.asList(preferredNetworks))) {
                    return createExternalEndpoint(externalEndpoint, ip);
                }
            }
        }
        throw new ParseEndpointException("No ExternalEndpoints value matches.");
    }

    private static boolean isPreferredNetwork(String ip, List<String> preferredNetworks) {
        return preferredNetworks.stream()
                .anyMatch(regex -> StringUtils.isNotBlank(regex) && (ip.matches(regex) || ip.startsWith(regex)));
    }

    private static Node.ExternalEndpoint createExternalEndpoint(
            LinkedHashMap<String, Object> externalEndpoint, String ip) {
        int controlPort = Integer.parseInt(externalEndpoint.get("controlPort").toString());
        int transactionPort =
                Integer.parseInt(externalEndpoint.get("transactionPort").toString());
        return new Node.ExternalEndpoint(ip, controlPort, transactionPort);
    }

    /**
     * Parse and validate CG addresses from configuration
     * @return true if CG mode should be enabled, false for single-raft mode
     */
    private static boolean initializeCgAddresses() {
        if (StringUtils.isBlank(CG_SERVER_ADDRESSES)) {
            LOGGER.info("No CG addresses configured, using single-raft mode");
            return false;
        }

        try {
            List<InetSocketAddress> cgAddressList = parseCgAddresses(CG_SERVER_ADDRESSES);
            if (CollectionUtils.isEmpty(cgAddressList)) {
                LOGGER.warn("CG addresses configured but empty after parsing, falling back to single-raft mode");
                return false;
            }

            // Store CG addresses with a default cluster name
            CG_ADDRESSES.put("controller", cgAddressList);
            LOGGER.info("CG mode enabled with {} controller addresses: {}", cgAddressList.size(), cgAddressList);
            return true;

        } catch (Exception e) {
            LOGGER.error("Failed to parse CG addresses: {}, falling back to single-raft mode", CG_SERVER_ADDRESSES, e);
            return false;
        }
    }

    /**
     * Parse CG addresses from comma-separated string
     */
    private static List<InetSocketAddress> parseCgAddresses(String cgAddresses) {
        if (StringUtils.isBlank(cgAddresses)) {
            return Collections.emptyList();
        }

        List<InetSocketAddress> addressList = new ArrayList<>();
        String[] addresses = cgAddresses.split(",");

        for (String address : addresses) {
            String trimmedAddress = address.trim();
            if (StringUtils.isBlank(trimmedAddress)) {
                continue;
            }

            try {
                InetSocketAddress socketAddress = parseSocketAddress(trimmedAddress);
                addressList.add(socketAddress);
                LOGGER.debug("Parsed CG address: {}", socketAddress);
            } catch (Exception e) {
                LOGGER.warn("Invalid CG address format: {}, skipping", trimmedAddress, e);
            }
        }

        return addressList;
    }

    /**
     * Parse individual socket address from string format "host:port"
     */
    private static InetSocketAddress parseSocketAddress(String address) {
        String[] parts = address.split(IP_PORT_SPLIT_CHAR);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid address format: " + address + ". Expected format: host:port");
        }

        String host = parts[0].trim();
        if (StringUtils.isBlank(host)) {
            throw new IllegalArgumentException("Host cannot be empty in address: " + address);
        }

        try {
            int port = Integer.parseInt(parts[1].trim());
            if (port <= 0 || port > 65535) {
                throw new IllegalArgumentException("Invalid port number in address: " + address + ". Port must be 1-65535");
            }
            return new InetSocketAddress(host, port);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port number in address: " + address, e);
        }
    }

    /**
     * Check if CG mode is enabled
     */
    private static boolean isCgModeEnabled() {
        return CG_MODE_ENABLED.get();
    }

    /**
     * Get available CG addresses for the controller cluster
     */
    private static List<InetSocketAddress> getCgAddresses() {
        return CG_ADDRESSES.getOrDefault("controller", Collections.emptyList());
    }

    /**
     * Select a random CG address for communication
     */
    private static String selectCgAddress() {
        List<InetSocketAddress> addresses = getCgAddresses();
        if (CollectionUtils.isEmpty(addresses)) {
            throw new IllegalStateException("No CG addresses available");
        }

        InetSocketAddress selected = addresses.get(ThreadLocalRandom.current().nextInt(addresses.size()));
        return NetUtil.toStringAddress(selected);
    }

    /**
     * Validate that CG mode is properly configured
     */
    private static void validateCgConfiguration() {
        if (!isCgModeEnabled()) {
            return; // single-raft mode, no validation needed
        }

        if (CollectionUtils.isEmpty(getCgAddresses())) {
            throw new IllegalStateException("CG mode enabled but no valid CG addresses configured");
        }

        LOGGER.info("CG configuration validation passed. Available CG addresses: {}", getCgAddresses().size());
    }

    /**
     * Initialize TXG cache management
     */
    private static void initializeTxgCacheManagement() {
        if (!isCgModeEnabled()) {
            return; // No TXG management needed in single-raft mode
        }

        if (TXG_MANAGEMENT_EXECUTOR == null) {
            synchronized (TXG_CACHE) {
                if (TXG_MANAGEMENT_EXECUTOR == null) {
                    TXG_MANAGEMENT_EXECUTOR = new ThreadPoolExecutor(
                            2,
                            4,
                            60L,
                            TimeUnit.SECONDS,
                            new LinkedBlockingQueue<>(),
                            new NamedThreadFactory("txgManagement", 1, true));

                    // Start TXG discovery and health check tasks
                    startTxgDiscoveryTask();
                    startTxgHealthCheckTask();

                    // Shutdown hook
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        if (TXG_MANAGEMENT_EXECUTOR != null) {
                            TXG_MANAGEMENT_EXECUTOR.shutdown();
                        }
                    }));

                    LOGGER.info("TXG cache management initialized");
                }
            }
        }
    }

    /**
     * Check if TXG cache needs refresh
     */
    private static boolean isTxgCacheStale() {
        return System.currentTimeMillis() - TXG_CACHE_LAST_UPDATE > CG_DISCOVERY_INTERVAL;
    }

    /**
     * Get cached TXG information
     */
    private static TxgInfo getCachedTxgInfo(String txgId) {
        return TXG_CACHE.get(txgId);
    }

    /**
     * Cache TXG information
     */
    private static void cacheTxgInfo(String txgId, TxgInfo txgInfo) {
        if (txgInfo != null) {
            TXG_CACHE.put(txgId, txgInfo);
            updateTxgHealthStatus(txgId, txgInfo.isHealthy());
            LOGGER.debug("Cached TXG info: {}", txgInfo);
        }
    }

    /**
     * Get TXG selection for service group
     */
    private static TxgSelectionInfo getTxgSelection(String serviceGroup) {
        TxgSelectionInfo selection = TXG_SELECTION_CACHE.get(serviceGroup);
        if (selection != null && selection.isExpired()) {
            // Remove expired selection
            TXG_SELECTION_CACHE.remove(serviceGroup);
            LOGGER.debug("Removed expired TXG selection for service group: {}", serviceGroup);
            return null;
        }
        return selection;
    }

    /**
     * Cache TXG selection for service group
     */
    private static void cacheTxgSelection(String serviceGroup, TxgSelectionInfo selection) {
        if (selection != null) {
            selection.setCacheExpiry(System.currentTimeMillis() + TXG_SELECTION_CACHE_TIME);
            TXG_SELECTION_CACHE.put(serviceGroup, selection);
            LOGGER.debug("Cached TXG selection: {}", selection);
        }
    }

    /**
     * Update TXG health status
     */
    private static void updateTxgHealthStatus(String txgId, boolean healthy) {
        if (healthy) {
            TXG_HEALTH_STATUS.put(txgId, System.currentTimeMillis());
        } else {
            TXG_HEALTH_STATUS.remove(txgId);
            // Mark cached TXG as unhealthy
            TxgInfo cachedInfo = TXG_CACHE.get(txgId);
            if (cachedInfo != null) {
                cachedInfo.setHealthy(false);
            }
        }
    }

    /**
     * Get available TXGs for service group
     */
    private static List<String> getAvailableTxgsForServiceGroup(String serviceGroup) {
        return SERVICE_GROUP_TXG_MAPPING.getOrDefault(serviceGroup, Collections.emptyList());
    }

    /**
     * Update service group to TXG mapping
     */
    private static void updateServiceGroupTxgMapping(String serviceGroup, List<String> txgIds) {
        if (CollectionUtils.isNotEmpty(txgIds)) {
            SERVICE_GROUP_TXG_MAPPING.put(serviceGroup, new ArrayList<>(txgIds));
            LOGGER.debug("Updated TXG mapping for service group {}: {}", serviceGroup, txgIds);
        } else {
            SERVICE_GROUP_TXG_MAPPING.remove(serviceGroup);
            LOGGER.debug("Removed TXG mapping for service group: {}", serviceGroup);
        }
    }

    /**
     * Clear expired cache entries
     */
    private static void cleanupExpiredCacheEntries() {
        // Cleanup expired TXG selections
        TXG_SELECTION_CACHE.entrySet().removeIf(entry -> {
            boolean expired = entry.getValue().isExpired();
            if (expired) {
                LOGGER.debug("Removed expired TXG selection for service group: {}", entry.getKey());
            }
            return expired;
        });

        // Cleanup stale TXG health status (older than 2 * health check interval)
        long staleThreshold = System.currentTimeMillis() - (2 * TXG_HEALTH_CHECK_INTERVAL);
        TXG_HEALTH_STATUS.entrySet().removeIf(entry -> {
            boolean stale = entry.getValue() < staleThreshold;
            if (stale) {
                LOGGER.debug("Removed stale health status for TXG: {}", entry.getKey());
            }
            return stale;
        });
    }

    /**
     * Get round robin counter for service group
     */
    private static AtomicInteger getRoundRobinCounter(String serviceGroup) {
        return ROUND_ROBIN_COUNTERS.computeIfAbsent(serviceGroup, k -> new AtomicInteger(0));
    }

    /**
     * Start TXG discovery background task
     */
    private static void startTxgDiscoveryTask() {
        TXG_MANAGEMENT_EXECUTOR.execute(() -> {
            while (!CLOSED.get()) {
                try {
                    if (isTxgCacheStale()) {
                        refreshTxgCacheFromCg();
                    }
                    Thread.sleep(CG_DISCOVERY_INTERVAL);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    LOGGER.error("Error in TXG discovery task", e);
                    try {
                        Thread.sleep(5000); // Wait before retry
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            LOGGER.info("TXG discovery task stopped");
        });
    }

    @Override
    public void close() {
        CLOSED.compareAndSet(false, true);
    }

    @Override
    public List<InetSocketAddress> aliveLookup(String transactionServiceGroup) {
        if (isCgModeEnabled()) {
            return aliveLookupViaCg(transactionServiceGroup);
        } else {
            return aliveLookupTraditional(transactionServiceGroup);
        }
    }

    /**
     * CG-based alive lookup
     */
    private static List<InetSocketAddress> aliveLookupViaCg(String transactionServiceGroup) {
        LOGGER.debug("CG-based alive lookup for service group: {}", transactionServiceGroup);

        try {
            // Get current TXG selection for this service group
            TxgSelectionInfo selection = getTxgSelection(transactionServiceGroup);

            if (selection != null && isValidTxgSelection(selection)) {
                // Validate that the selected TXG is still healthy
                TxgInfo txgInfo = getCachedTxgInfo(selection.getSelectedTxgId());

                if (txgInfo != null && txgInfo.isReady()) {
                    List<InetSocketAddress> endpoints = getHealthyEndpointsFromTxg(txgInfo);

                    if (CollectionUtils.isNotEmpty(endpoints)) {
                        LOGGER.debug("CG-based alive lookup found {} healthy endpoints for service group: {}",
                                endpoints.size(), transactionServiceGroup);
                        return endpoints;
                    }
                } else {
                    // Selected TXG is not healthy, invalidate selection
                    LOGGER.warn("Selected TXG {} is not healthy, invalidating selection for service group: {}",
                            selection.getSelectedTxgId(), transactionServiceGroup);
                    invalidateTxgSelection(transactionServiceGroup);
                }
            }

            // No valid selection or selected TXG is unhealthy, try to select a new one
            TxgSelectionInfo newSelection = getOrSelectTxg(transactionServiceGroup);
            if (newSelection != null && CollectionUtils.isNotEmpty(newSelection.getTransactionEndpoints())) {
                LOGGER.info("Selected new TXG {} for service group: {}",
                        newSelection.getSelectedTxgId(), transactionServiceGroup);
                return newSelection.getTransactionEndpoints();
            }

        } catch (Exception e) {
            LOGGER.error("CG-based alive lookup failed for service group: {}", transactionServiceGroup, e);
        }

        LOGGER.warn("No alive TXG endpoints found for service group: {}", transactionServiceGroup);
        return Collections.emptyList();
    }

    /**
     * Get healthy endpoints from TXG
     */
    private static List<InetSocketAddress> getHealthyEndpointsFromTxg(TxgInfo txgInfo) {
        List<InetSocketAddress> endpoints = new ArrayList<>();

        // Always include leader if healthy
        if (txgInfo.getLeader() != null && txgInfo.isHealthy()) {
            InetSocketAddress leaderEndpoint = txgInfo.getLeaderTransactionEndpoint();
            if (leaderEndpoint != null) {
                endpoints.add(leaderEndpoint);
            }
        }
        // TODO: Add follower endpoints for read operations if supported?
        return endpoints;
    }

    public List<InetSocketAddress> aliveLookupTraditional(String transactionServiceGroup) {
        if (METADATA.isRaftMode()) {
            String clusterName = getServiceGroup(transactionServiceGroup);
            Node leader = METADATA.getLeader(clusterName);
            if (leader != null) {
                return Collections.singletonList(selectTransactionEndpoint(leader));
            }
        }
        return RegistryService.super.aliveLookup(transactionServiceGroup);
    }

    /**
     * Enhanced watch method that supports both traditional and CG-based watching
     */
    private static boolean watch() throws RetryableException {
        if (isCgModeEnabled()) {
            return watchCgForChanges();
        } else {
            return watchTraditionalCluster();
        }
    }

    /**
     * Watch CG for TXG changes (CG-based mode)
     */
    private static boolean watchCgForChanges() throws RetryableException {
        try {
            // Collect current terms for all service groups we're interested in, not sure if this will be correct
            Map<String, Long> serviceGroupTerms = collectServiceGroupTerms();

            if (serviceGroupTerms.isEmpty()) {
                // No service groups to watch, return false to trigger refresh?
                return false;
            }

            // Prepare watch request for CG
            Map<String, String> headers = createCgRequestHeaders();
            headers.put(HTTP.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

            Map<String, Object> watchRequest = new HashMap<>();

            // Convert service group terms to TXG terms by looking up current selections
            Map<String, Long> txgTerms = new HashMap<>();
            for (Map.Entry<String, Long> entry : serviceGroupTerms.entrySet()) {
                String serviceGroup = entry.getKey();
                TxgSelectionInfo selection = getTxgSelection(serviceGroup);
                if (selection != null) {
                    TxgInfo txgInfo = getCachedTxgInfo(selection.getSelectedTxgId());
                    if (txgInfo != null) {
                        txgTerms.put(selection.getSelectedTxgId(), txgInfo.getTerm());
                    }
                }
            }

            watchRequest.put("txgTerms", txgTerms);

            String cgAddress = selectHealthyCgEndpoint();
            String requestBody = OBJECT_MAPPER.writeValueAsString(watchRequest);

            LOGGER.debug("Watching CG for TXG changes: {} TXGs from {}", txgTerms.size(), cgAddress);

            try (CloseableHttpResponse response = HttpClientUtil.doPost(
                    "http://" + cgAddress + "/metadata/v1/watch/txgroups",
                    requestBody, headers, 30000)) {

                if (response != null) {
                    StatusLine statusLine = response.getStatusLine();
                    int statusCode = statusLine.getStatusCode();

                    if (statusCode == HttpStatus.SC_UNAUTHORIZED) {
                        handleCgAuthenticationError();
                    } else if (statusCode == HttpStatus.SC_OK) {
                        // Parse response to see what changed
                        String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                        handleCgWatchResponse(responseBody);
                        return true;
                    } else {
                        LOGGER.debug("CG watch returned status: {}", statusCode);
                    }

                    return statusCode == HttpStatus.SC_OK;
                }
            }

        } catch (IOException e) {
            LOGGER.debug("CG watch request failed: {}", e.getMessage());
            throw new RetryableException("CG watch failed", e);
        } catch (Exception e) {
            LOGGER.error("Unexpected error in CG watch", e);
            throw new RetryableException("CG watch error", e);
        }

        return false;
    }

    /**
     * Collect current terms for service groups we're watching
     */
    private static Map<String, Long> collectServiceGroupTerms() {
        Map<String, Long> terms = new HashMap<>();

        // Add current transaction service group
        if (StringUtils.isNotBlank(CURRENT_TRANSACTION_SERVICE_GROUP)) {
            TxgSelectionInfo selection = getTxgSelection(CURRENT_TRANSACTION_SERVICE_GROUP);
            if (selection != null) {
                TxgInfo txgInfo = getCachedTxgInfo(selection.getSelectedTxgId());
                if (txgInfo != null) {
                    terms.put(CURRENT_TRANSACTION_SERVICE_GROUP, txgInfo.getTerm());
                }
            }
        }

        // Add any other service groups from the selection cache
        for (Map.Entry<String, TxgSelectionInfo> entry : TXG_SELECTION_CACHE.entrySet()) {
            String serviceGroup = entry.getKey();
            TxgSelectionInfo selection = entry.getValue();

            if (!terms.containsKey(serviceGroup) && selection != null && !selection.isExpired()) {
                TxgInfo txgInfo = getCachedTxgInfo(selection.getSelectedTxgId());
                if (txgInfo != null) {
                    terms.put(serviceGroup, txgInfo.getTerm());
                }
            }
        }

        return terms;
    }

    /**
     * Handle CG watch response
     */
    private static void handleCgWatchResponse(String responseBody) {
        try {
            if (StringUtils.isBlank(responseBody)) {
                return;
            }

            JsonNode rootNode = OBJECT_MAPPER.readTree(responseBody);

            // Check for TXG changes
            if (rootNode.has("changedTxgs")) {
                JsonNode changedTxgsNode = rootNode.get("changedTxgs");

                if (changedTxgsNode.isArray()) {
                    for (JsonNode txgChangeNode : changedTxgsNode) {
                        String txgId = txgChangeNode.get("txgId").asText();
                        long newTerm = txgChangeNode.get("newTerm").asLong();

                        LOGGER.info("CG reported TXG change: {} (term: {})", txgId, newTerm);

                        // Handle the TXG change
                        handleTxgMetadataChange(txgId, newTerm);
                    }
                }
            }

            // Check for service group mapping changes
            if (rootNode.has("serviceGroupChanges")) {
                JsonNode serviceGroupChangesNode = rootNode.get("serviceGroupChanges");

                if (serviceGroupChangesNode.isArray()) {
                    for (JsonNode changeNode : serviceGroupChangesNode) {
                        String serviceGroup = changeNode.get("serviceGroup").asText();
                        LOGGER.info("CG reported service group mapping change: {}", serviceGroup);

                        // Invalidate selection for this service group to force re-selection
                        invalidateTxgSelection(serviceGroup);
                    }
                }
            }

        } catch (JsonProcessingException e) {
            LOGGER.error("Failed to parse CG watch response", e);
        }
    }

    private static boolean watchTraditionalCluster() throws RetryableException {
        Map<String, String> header = new HashMap<>();
        header.put(HTTP.CONTENT_TYPE, ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
        Map<String, String> param = new HashMap<>();
        String clusterName = CURRENT_TRANSACTION_CLUSTER_NAME;
        Map<String, Long> groupTerms = METADATA.getClusterTerm(clusterName);
        groupTerms.forEach((k, v) -> param.put(k, String.valueOf(v)));
        for (String group : groupTerms.keySet()) {
            String tcAddress = queryHttpAddress(clusterName, group);
            if (isTokenExpired()) {
                refreshToken(tcAddress);
            }
            if (StringUtils.isNotBlank(jwtToken)) {
                header.put(AUTHORIZATION_HEADER, jwtToken);
            }
            try (CloseableHttpResponse response =
                    HttpClientUtil.doPost("http://" + tcAddress + "/metadata/v1/watch", param, header, 30000)) {
                if (response != null) {
                    StatusLine statusLine = response.getStatusLine();
                    if (statusLine != null && statusLine.getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
                        if (StringUtils.isNotBlank(USERNAME) && StringUtils.isNotBlank(PASSWORD)) {
                            throw new RetryableException("Authentication failed!");
                        } else {
                            throw new AuthenticationFailedException(
                                    "Authentication failed! you should configure the correct username and password.");
                        }
                    }
                    return statusLine != null && statusLine.getStatusCode() == HttpStatus.SC_OK;
                }
            } catch (IOException e) {
                LOGGER.error("watch cluster node: {}, fail: {}", tcAddress, e.getMessage());
                throw new RetryableException(e.getMessage(), e);
            }
            break;
        }
        return false;
    }

    @Override
    public List<InetSocketAddress> refreshAliveLookup(
            String transactionServiceGroup, List<InetSocketAddress> aliveAddress) {
        if (isCgModeEnabled()) {
            return refreshAliveLookupViaCg(transactionServiceGroup, aliveAddress);
        } else {
            return refreshAliveLookupTraditional(transactionServiceGroup, aliveAddress);
        }
    }

    /**
     * CG-based refresh alive lookup
     */
    private static List<InetSocketAddress> refreshAliveLookupViaCg(
            String transactionServiceGroup, List<InetSocketAddress> aliveAddress) {

        LOGGER.debug("Refreshing alive lookup via CG for service group: {}", transactionServiceGroup);

        try {
            // Get current TXG selection
            TxgSelectionInfo selection = getTxgSelection(transactionServiceGroup);

            if (selection != null) {
                String selectedTxgId = selection.getSelectedTxgId();

                // Refresh TXG metadata to get latest health status
                TxgInfo refreshedTxgInfo = getValidatedTxgInfo(selectedTxgId);

                if (refreshedTxgInfo != null) {
                    // Update health status based on alive addresses
                    boolean isHealthy = updateTxgHealthFromAliveAddresses(refreshedTxgInfo, aliveAddress);

                    if (isHealthy) {
                        // TXG is healthy, filter alive addresses to only include the leader
                        List<InetSocketAddress> filteredAddresses = filterAddressesForTxg(
                                refreshedTxgInfo, aliveAddress);

                        // Update cache and return filtered addresses
                        ALIVE_NODES.put(transactionServiceGroup, filteredAddresses);

                        LOGGER.debug("Updated alive addresses for service group {}: {} addresses",
                                transactionServiceGroup, filteredAddresses.size());
                        return filteredAddresses;
                    } else {
                        // TXG is unhealthy, invalidate selection and try to select new one
                        LOGGER.warn("TXG {} is unhealthy, invalidating selection for service group: {}",
                                selectedTxgId, transactionServiceGroup);

                        invalidateTxgSelection(transactionServiceGroup);
                        refreshedTxgInfo.setHealthy(false);

                        // Try to select a new healthy TXG
                        TxgSelectionInfo newSelection = getOrSelectTxg(transactionServiceGroup);
                        if (newSelection != null) {
                            LOGGER.info("Selected new healthy TXG {} for service group: {}",
                                    newSelection.getSelectedTxgId(), transactionServiceGroup);
                            return newSelection.getTransactionEndpoints();
                        }
                    }
                }
            }

            // No valid TXG selection, return empty list
            ALIVE_NODES.put(transactionServiceGroup, Collections.emptyList());
            return Collections.emptyList();

        } catch (Exception e) {
            LOGGER.error("Error refreshing alive lookup for service group: {}", transactionServiceGroup, e);
            ALIVE_NODES.put(transactionServiceGroup, Collections.emptyList());
            return Collections.emptyList();
        }
    }

    public List<InetSocketAddress> refreshAliveLookupTraditional(
            String transactionServiceGroup, List<InetSocketAddress> aliveAddress) {
        if (METADATA.isRaftMode()) {
            Node leader = METADATA.getLeader(getServiceGroup(transactionServiceGroup));
            InetSocketAddress leaderAddress = selectTransactionEndpoint(leader);
            return ALIVE_NODES.put(
                    transactionServiceGroup,
                    aliveAddress.isEmpty()
                            ? aliveAddress
                            : aliveAddress.parallelStream()
                                    .filter(inetSocketAddress -> {
                                        // Since only follower will turn into leader, only the follower node needs to be
                                        // listened to
                                        return inetSocketAddress.getPort() != leaderAddress.getPort()
                                                || !inetSocketAddress
                                                        .getAddress()
                                                        .getHostAddress()
                                                        .equals(leaderAddress
                                                                .getAddress()
                                                                .getHostAddress());
                                    })
                                    .collect(Collectors.toList()));
        } else {
            return RegistryService.super.refreshAliveLookup(transactionServiceGroup, aliveAddress);
        }
    }

    /**
     * Update TXG health status based on alive addresses
     */
    private static boolean updateTxgHealthFromAliveAddresses(TxgInfo txgInfo, List<InetSocketAddress> aliveAddresses) {
        if (txgInfo == null || CollectionUtils.isEmpty(aliveAddresses)) {
            return false;
        }

        // Check if TXG leader endpoint is in alive addresses
        InetSocketAddress leaderEndpoint = txgInfo.getLeaderTransactionEndpoint();
        if (leaderEndpoint == null) {
            return false;
        }

        boolean leaderAlive = aliveAddresses.contains(leaderEndpoint);

        // Update health status
        txgInfo.setHealthy(leaderAlive);
        updateTxgHealthStatus(txgInfo.getTxgId(), leaderAlive);

        if (!leaderAlive) {
            LOGGER.warn("TXG {} leader is not alive: {}", txgInfo.getTxgId(), leaderEndpoint);
        }

        return leaderAlive;
    }

    /**
     * Filter alive addresses to only include addresses from the selected TXG
     */
    private static List<InetSocketAddress> filterAddressesForTxg(
            TxgInfo txgInfo, List<InetSocketAddress> aliveAddresses) {

        if (txgInfo == null || CollectionUtils.isEmpty(aliveAddresses)) {
            return Collections.emptyList();
        }

        Set<InetSocketAddress> txgEndpoints = new HashSet<>();

        // Add leader endpoint
        InetSocketAddress leaderEndpoint = txgInfo.getLeaderTransactionEndpoint();
        if (leaderEndpoint != null) {
            txgEndpoints.add(leaderEndpoint);
        }

        // TODO: Add follower endpoints if read operations are supported?
        return aliveAddresses.stream()
                .filter(txgEndpoints::contains)
                .collect(Collectors.toList());
    }

    /**
     * Handle TXG health check results
     */
    private static void processTxgHealthCheckResults() {
        if (!isCgModeEnabled()) {
            return;
        }

        long now = System.currentTimeMillis();
        long healthCheckThreshold = now - TXG_HEALTH_CHECK_INTERVAL;

        // Check each cached TXG
        for (Map.Entry<String, TxgInfo> entry : TXG_CACHE.entrySet()) {
            String txgId = entry.getKey();
            TxgInfo txgInfo = entry.getValue();

            Long lastHealthCheck = TXG_HEALTH_STATUS.get(txgId);
            boolean wasHealthy = txgInfo.isHealthy();

            // Mark as unhealthy if no recent health check
            boolean isHealthy = lastHealthCheck != null && lastHealthCheck > healthCheckThreshold;

            if (wasHealthy != isHealthy) {
                txgInfo.setHealthy(isHealthy);

                LOGGER.info("TXG {} health status changed: {} -> {}", txgId, wasHealthy, isHealthy);

                if (!isHealthy) {
                    // Invalidate any selections using this unhealthy TXG
                    invalidateTxgSelectionForTxg(txgId);
                }
            }
        }
    }

    /**
     * Update the existing startTxgHealthCheckTask to include health processing
     */
    private static void startTxgHealthCheckTask() {
        TXG_MANAGEMENT_EXECUTOR.execute(() -> {
            while (!CLOSED.get()) {
                try {
                    cleanupExpiredCacheEntries();
                    processTxgHealthCheckResults(); // Added health check processing
                    Thread.sleep(TXG_HEALTH_CHECK_INTERVAL);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    LOGGER.error("Error in TXG health check task", e);
                    try {
                        Thread.sleep(5000); // Wait before retry
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            LOGGER.info("TXG health check task stopped");
        });
    }

    private static void acquireClusterMetaDataByClusterName(String clusterName) {
        try {
            acquireClusterMetaData(clusterName, "");
        } catch (RetryableException e) {
            LOGGER.warn(e.getMessage(), e);
        }
    }

    private static void acquireClusterMetaData(String clusterName, String group) throws RetryableException {
        String tcAddress = queryHttpAddress(clusterName, group);
        Map<String, String> header = new HashMap<>();
        header.put(HTTP.CONTENT_TYPE, ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
        if (isTokenExpired()) {
            refreshToken(tcAddress);
        }
        if (StringUtils.isNotBlank(jwtToken)) {
            header.put(AUTHORIZATION_HEADER, jwtToken);
        }
        if (StringUtils.isNotBlank(tcAddress)) {
            Map<String, String> param = new HashMap<>();
            param.put("group", group);
            String response = null;
            try (CloseableHttpResponse httpResponse =
                    HttpClientUtil.doGet("http://" + tcAddress + "/metadata/v1/cluster", param, header, 1000)) {
                if (httpResponse != null) {
                    int statusCode = httpResponse.getStatusLine().getStatusCode();
                    if (statusCode == HttpStatus.SC_OK) {
                        response = EntityUtils.toString(httpResponse.getEntity(), StandardCharsets.UTF_8);
                    } else if (statusCode == HttpStatus.SC_UNAUTHORIZED) {
                        if (StringUtils.isNotBlank(USERNAME) && StringUtils.isNotBlank(PASSWORD)) {
                            refreshToken(tcAddress);
                            throw new RetryableException("Token refreshed, retrying request.");
                        } else {
                            throw new AuthenticationFailedException(
                                    "Authentication failed! you should configure the correct username and password.");
                        }
                    } else {
                        throw new AuthenticationFailedException(
                                "Authentication failed! you should configure the correct username and password.");
                    }
                }
                MetadataResponse metadataResponse;
                if (StringUtils.isNotBlank(response)) {
                    try {
                        metadataResponse = OBJECT_MAPPER.readValue(response, MetadataResponse.class);
                        METADATA.refreshMetadata(clusterName, metadataResponse);
                    } catch (JsonProcessingException e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                }
            } catch (IOException e) {
                throw new RetryableException(e.getMessage(), e);
            }
        }
    }

    private static void refreshToken(String tcAddress) throws RetryableException {
        // if username and password is not in config , return
        if (StringUtils.isBlank(USERNAME) || StringUtils.isBlank(PASSWORD)) {
            return;
        }
        // get token and set it in cache
        Map<String, String> param = new HashMap<>();
        param.put(PRO_USERNAME_KEY, USERNAME);
        param.put(PRO_PASSWORD_KEY, PASSWORD);
        Map<String, String> header = new HashMap<>();
        header.put(HTTP.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
        String response = null;
        try (CloseableHttpResponse httpResponse =
                HttpClientUtil.doPost("http://" + tcAddress + "/api/v1/auth/login", param, header, 1000)) {
            if (httpResponse != null) {
                if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    response = EntityUtils.toString(httpResponse.getEntity(), StandardCharsets.UTF_8);
                    JsonNode jsonNode = OBJECT_MAPPER.readTree(response);
                    String codeStatus = jsonNode.get("code").asText();
                    if (!StringUtils.equals(codeStatus, "200")) {
                        // authorized failed,throw exception to kill process
                        throw new AuthenticationFailedException(
                                "Authentication failed! you should configure the correct username and password.");
                    }
                    jwtToken = jsonNode.get("data").asText();
                    tokenTimeStamp = System.currentTimeMillis();
                } else {
                    // authorized failed,throw exception to kill process
                    throw new AuthenticationFailedException(
                            "Authentication failed! you should configure the correct username and password.");
                }
            }
        } catch (IOException e) {
            throw new RetryableException(e.getMessage(), e);
        }
    }

    @Override
    public List<InetSocketAddress> lookup(String key) throws Exception {
        String clusterName = getServiceGroup(key);
        if (clusterName == null) {
            return null;
        }

        CURRENT_TRANSACTION_SERVICE_GROUP = key;
        CURRENT_TRANSACTION_CLUSTER_NAME = clusterName;

        // CG-based discovery flow
        if (isCgModeEnabled()) {
            return lookupViaCg(key, clusterName);
        } else {
            // Traditional single-raft discovery flow
            return lookupTraditional(key, clusterName);
        }
    }

    /**
     * CG-based lookup flow
     */
    private static List<InetSocketAddress> lookupViaCg(String serviceGroup, String clusterName) throws Exception {
        LOGGER.debug("Using CG-based lookup for service group: {}", serviceGroup);

        try {
            // Check if we have TXG information cached
            if (!hasCachedTxgInfoForServiceGroup(serviceGroup)) {
                LOGGER.debug("No cached TXG info for service group {}, initializing from CG", serviceGroup);
                initializeTxgCacheForServiceGroup(serviceGroup);
            }

            // Get TXG endpoints using load balancing
            List<InetSocketAddress> endpoints = getTxgEndpointsForServiceGroup(serviceGroup);

            if (CollectionUtils.isNotEmpty(endpoints)) {
                // Start TXG monitoring if not already started
                if (!isMetadataRefreshActive()) {
                    startCgBasedMetadataRefresh();
                }

                LOGGER.info("CG-based lookup successful for service group {}: {} endpoints",
                        serviceGroup, endpoints.size());
                return endpoints;
            } else {
                LOGGER.warn("No TXG endpoints available for service group: {}", serviceGroup);

                // Fallback: try direct CG query
                return fallbackDirectCgQuery(serviceGroup);
            }

        } catch (Exception e) {
            LOGGER.error("CG-based lookup failed for service group: {}", serviceGroup, e);

            // If CG lookup fails completely, could fallback to traditional mode if configured
            if (shouldFallbackToTraditional()) {
                LOGGER.warn("Falling back to traditional lookup for service group: {}", serviceGroup);
                return lookupTraditional(serviceGroup, clusterName);
            }

            throw new Exception("Service discovery failed for service group: " + serviceGroup, e);
        }
    }

    /**
     * Traditional single-raft lookup flow (existing logic)
     */
    private static List<InetSocketAddress> lookupTraditional(String serviceGroup, String clusterName) throws Exception {
        LOGGER.debug("Using traditional lookup for service group: {}", serviceGroup);

        if (!METADATA.containsGroup(clusterName)) {
            String raftClusterAddress = CONFIG.getConfig(getRaftAddrFileKey());
            if (StringUtils.isNotBlank(raftClusterAddress)) {
                List<InetSocketAddress> list = new ArrayList<>();
                String[] addresses = raftClusterAddress.split(",");
                for (String address : addresses) {
                    String[] endpoint = address.split(IP_PORT_SPLIT_CHAR);
                    String host = endpoint[0];
                    int port = Integer.parseInt(endpoint[1]);
                    list.add(new InetSocketAddress(host, port));
                }
                if (CollectionUtils.isEmpty(list)) {
                    return null;
                }
                INIT_ADDRESSES.put(clusterName, list);
                // init jwt token
                try {
                    refreshToken(queryHttpAddress(clusterName, serviceGroup));
                } catch (Exception e) {
                    throw new RuntimeException("Init fetch token failed!", e);
                }
                // Refresh the metadata by initializing the address
                acquireClusterMetaDataByClusterName(clusterName);
                startQueryMetadata();
            }
        }

        List<Node> nodes = METADATA.getNodes(clusterName);
        if (CollectionUtils.isNotEmpty(nodes)) {
            return nodes.parallelStream()
                    .map(RaftRegistryServiceImpl::selectTransactionEndpoint)
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    /**
     * Check if we have cached TXG information for service group
     */
    private static boolean hasCachedTxgInfoForServiceGroup(String serviceGroup) {
        List<String> txgIds = getAvailableTxgsForServiceGroup(serviceGroup);
        if (CollectionUtils.isEmpty(txgIds)) {
            return false;
        }

        // Check if at least one TXG has valid cached info
        return txgIds.stream().anyMatch(txgId -> {
            TxgInfo info = getCachedTxgInfo(txgId);
            return info != null && info.isReady();
        });
    }

    /**
     * Fallback direct CG query when cached data is unavailable
     */
    private static List<InetSocketAddress> fallbackDirectCgQuery(String serviceGroup) {
        try {
            LOGGER.debug("Attempting direct CG query fallback for service group: {}", serviceGroup);

            // Force refresh from CG
            List<String> txgIds = discoverTxgsForServiceGroup(serviceGroup);
            if (CollectionUtils.isNotEmpty(txgIds)) {
                // Try to get at least one healthy TXG
                for (String txgId : txgIds) {
                    try {
                        TxgInfo txgInfo = getTxgMetadataFromCg(txgId);
                        if (txgInfo != null && txgInfo.isReady()) {
                            cacheTxgInfo(txgId, txgInfo);
                            InetSocketAddress endpoint = txgInfo.getLeaderTransactionEndpoint();
                            if (endpoint != null) {
                                LOGGER.info("Fallback CG query successful for service group {}: {}",
                                        serviceGroup, endpoint);
                                return Collections.singletonList(endpoint);
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.warn("Failed to query TXG {} in fallback", txgId, e);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Fallback direct CG query failed for service group: {}", serviceGroup, e);
        }

        return Collections.emptyList();
    }

    /**
     * Check if should fallback to traditional mode on CG failure
     */
    private static boolean shouldFallbackToTraditional() {
        // Check if traditional mode addresses are configured as backup
        String traditionalAddress = CONFIG.getConfig(getRaftAddrFileKey());
        return StringUtils.isNotBlank(traditionalAddress);
    }

    /**
     * Check if metadata refresh is active
     */
    private static boolean isMetadataRefreshActive() {
        return TXG_MANAGEMENT_EXECUTOR != null && !TXG_MANAGEMENT_EXECUTOR.isShutdown();
    }

    /**
     * Start CG-based metadata refresh
     */
    private static void startCgBasedMetadataRefresh() {
        if (REFRESH_METADATA_EXECUTOR == null) {
            synchronized (INIT_ADDRESSES) {
                if (REFRESH_METADATA_EXECUTOR == null) {
                    REFRESH_METADATA_EXECUTOR = new ThreadPoolExecutor(
                            1,
                            1,
                            0L,
                            TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue<>(),
                            new NamedThreadFactory("cgMetadataRefresh", 1, true));

                    REFRESH_METADATA_EXECUTOR.execute(() -> {
                        LOGGER.info("Starting CG-based metadata refresh loop");

                        while (!CLOSED.get()) {
                            try {
                                // Check for TXG changes via CG watch
                                boolean hasChanges = watchForCgTxgChanges();

                                // Force refresh if cache is stale
                                if (hasChanges || isTxgCacheStale()) {
                                    refreshTxgCacheFromCg();
                                }

                                Thread.sleep(Math.min(CG_DISCOVERY_INTERVAL, 10000)); // Max 10s sleep

                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                break;
                            } catch (Exception e) {
                                LOGGER.error("Error in CG metadata refresh loop", e);
                                try {
                                    Thread.sleep(5000); // Wait before retry
                                } catch (InterruptedException ie) {
                                    Thread.currentThread().interrupt();
                                    break;
                                }
                            }
                        }

                        LOGGER.info("CG-based metadata refresh loop stopped");
                    });

                    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        CLOSED.compareAndSet(false, true);
                        if (REFRESH_METADATA_EXECUTOR != null) {
                            REFRESH_METADATA_EXECUTOR.shutdown();
                        }
                    }));
                }
            }
        }
    }

    /**
     * Watch for TXG changes via CG
     */
    private static boolean watchForCgTxgChanges() {
        if (!isCgModeEnabled()) {
            return false;
        }

        try {
            // Collect current terms for all cached TXGs
            Map<String, Long> txgTerms = TXG_CACHE.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> entry.getValue().getTerm()
                    ));

            if (txgTerms.isEmpty()) {
                // No cached TXGs to watch
                return false;
            }

            // Watch for changes
            return watchCgForTxgChanges(txgTerms);

        } catch (RetryableException e) {
            LOGGER.debug("CG watch request failed (will retry): {}", e.getMessage());
            return false;
        } catch (Exception e) {
            LOGGER.error("Unexpected error in CG watch", e);
            return false;
        }
    }



    private static String getMetadataMaxAgeMs() {
        return String.join(
                ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR,
                ConfigurationKeys.FILE_ROOT_REGISTRY,
                REGISTRY_TYPE,
                META_DATA_MAX_AGE_MS);
    }

    /**
     * Select optimal TXG for service group using configured load balancing strategy
     */
    private static TxgInfo selectTxgForServiceGroup(String serviceGroup) {
        List<String> availableTxgIds = getAvailableTxgsForServiceGroup(serviceGroup);
        if (CollectionUtils.isEmpty(availableTxgIds)) {
            LOGGER.warn("No TXGs available for service group: {}", serviceGroup);
            return null;
        }

        // Filter healthy TXGs
        List<TxgInfo> healthyTxgs = availableTxgIds.stream()
                .map(TXG_CACHE::get)
                .filter(txg -> txg != null && txg.isReady())
                .collect(Collectors.toList());

        if (CollectionUtils.isEmpty(healthyTxgs)) {
            LOGGER.warn("No healthy TXGs available for service group: {}", serviceGroup);
            return null;
        }

        LoadBalanceStrategy strategy = LoadBalanceStrategy.fromCode(TXG_LOAD_BALANCE_STRATEGY);
        LOGGER.debug("Selecting TXG for service group {} using strategy: {}", serviceGroup, strategy);

        return selectTxgByStrategy(serviceGroup, healthyTxgs, strategy);
    }

    /**
     * Select TXG based on load balancing strategy
     */
    private static TxgInfo selectTxgByStrategy(String serviceGroup, List<TxgInfo> availableTxgs, LoadBalanceStrategy strategy) {
        if (CollectionUtils.isEmpty(availableTxgs)) {
            return null;
        }

        if (availableTxgs.size() == 1) {
            return availableTxgs.get(0);
        }

        switch (strategy) {
            case ROUND_ROBIN:
                return selectTxgRoundRobin(serviceGroup, availableTxgs);
            case WEIGHTED:
                return selectTxgWeighted(availableTxgs);
            case RANDOM:
                return selectTxgRandom(availableTxgs);
            case STICKY:
                return selectTxgSticky(serviceGroup, availableTxgs);
            case LEAST_CONNECTIONS:
                return selectTxgLeastConnections(availableTxgs);
            default:
                LOGGER.warn("Unknown load balance strategy: {}, falling back to round robin", strategy);
                return selectTxgRoundRobin(serviceGroup, availableTxgs);
        }
    }

    /**
     * Round robin TXG selection
     */
    private static TxgInfo selectTxgRoundRobin(String serviceGroup, List<TxgInfo> availableTxgs) {
        AtomicInteger counter = getRoundRobinCounter(serviceGroup);
        int index = Math.abs(counter.getAndIncrement()) % availableTxgs.size();
        TxgInfo selected = availableTxgs.get(index);

        LOGGER.debug("Round robin selected TXG {} for service group {} (index: {})",
                selected.getTxgId(), serviceGroup, index);
        return selected;
    }

    /**
     * Weighted TXG selection based on TXG weights
     */
    private static TxgInfo selectTxgWeighted(List<TxgInfo> availableTxgs) {
        // Calculate total weight
        int totalWeight = availableTxgs.stream()
                .mapToInt(TxgInfo::getWeight)
                .sum();

        if (totalWeight <= 0) {
            // Fallback to random if no weights configured
            return selectTxgRandom(availableTxgs);
        }

        // Generate random number within total weight
        int randomWeight = ThreadLocalRandom.current().nextInt(totalWeight);
        int currentWeight = 0;

        for (TxgInfo txg : availableTxgs) {
            currentWeight += txg.getWeight();
            if (randomWeight < currentWeight) {
                LOGGER.debug("Weighted selection chose TXG {} (weight: {}/{})",
                        txg.getTxgId(), txg.getWeight(), totalWeight);
                return txg;
            }
        }

        // Fallback to last TXG (shouldn't happen)
        return availableTxgs.get(availableTxgs.size() - 1);
    }

    /**
     * Random TXG selection
     */
    private static TxgInfo selectTxgRandom(List<TxgInfo> availableTxgs) {
        int index = ThreadLocalRandom.current().nextInt(availableTxgs.size());
        TxgInfo selected = availableTxgs.get(index);

        LOGGER.debug("Random selection chose TXG {} (index: {})", selected.getTxgId(), index);
        return selected;
    }

    /**
     * Sticky TXG selection based on service group hash
     */
    private static TxgInfo selectTxgSticky(String serviceGroup, List<TxgInfo> availableTxgs) {
        // Use consistent hashing based on service group name
        int hash = Math.abs(serviceGroup.hashCode());
        int index = hash % availableTxgs.size();
        TxgInfo selected = availableTxgs.get(index);

        LOGGER.debug("Sticky selection chose TXG {} for service group {} (hash: {})",
                selected.getTxgId(), serviceGroup, hash);
        return selected;
    }

    /**
     * Least connections TXG selection (simplified implementation)
     * Note: This is a placeholder implementation. In a real scenario,
     * we would need connection count metrics from the TXGs.
     * we can also just remove this strategy as i think we have more than enough
     */
    private static TxgInfo selectTxgLeastConnections(List<TxgInfo> availableTxgs) {
        // For now, select the TXG with the highest weight (assuming higher weight = more capacity)
        TxgInfo selected = availableTxgs.stream()
                .max((txg1, txg2) -> Integer.compare(txg1.getWeight(), txg2.getWeight()))
                .orElse(availableTxgs.get(0));

        LOGGER.debug("Least connections selection chose TXG {} (weight: {})",
                selected.getTxgId(), selected.getWeight());
        return selected;
    }

    /**
     * Create TXG selection info from selected TXG
     */
    private static TxgSelectionInfo createTxgSelection(String serviceGroup, TxgInfo selectedTxg) {
        if (selectedTxg == null || !selectedTxg.isReady()) {
            return null;
        }

        // Create list of transaction endpoints (for now, just the leader)
        List<InetSocketAddress> endpoints = new ArrayList<>();
        if (selectedTxg.getLeaderTransactionEndpoint() != null) {
            endpoints.add(selectedTxg.getLeaderTransactionEndpoint());
        }

        // TODO: Add follower endpoints if read operations are supported

        TxgSelectionInfo selection = new TxgSelectionInfo(serviceGroup, selectedTxg.getTxgId(), endpoints);
        selection.setSelectionStrategy(TXG_LOAD_BALANCE_STRATEGY);

        return selection;
    }

    /**
     * Validate TXG selection and ensure it's still valid
     */
    private static boolean isValidTxgSelection(TxgSelectionInfo selection) {
        if (selection == null || selection.isExpired()) {
            return false;
        }

        // Check if selected TXG is still healthy
        TxgInfo txgInfo = getCachedTxgInfo(selection.getSelectedTxgId());
        if (txgInfo == null || !txgInfo.isReady()) {
            LOGGER.debug("TXG selection invalid - TXG {} is not ready", selection.getSelectedTxgId());
            return false;
        }

        // Check if endpoints are still valid
        InetSocketAddress expectedEndpoint = txgInfo.getLeaderTransactionEndpoint();
        if (expectedEndpoint == null) {
            return false;
        }

        List<InetSocketAddress> cachedEndpoints = selection.getTransactionEndpoints();
        if (CollectionUtils.isEmpty(cachedEndpoints) || !cachedEndpoints.contains(expectedEndpoint)) {
            LOGGER.debug("TXG selection invalid - endpoints have changed for TXG {}", selection.getSelectedTxgId());
            return false;
        }

        return true;
    }

    /**
     * Get or select TXG for service group
     */
    private static TxgSelectionInfo getOrSelectTxg(String serviceGroup) {
        // Check cached selection first
        TxgSelectionInfo cachedSelection = getTxgSelection(serviceGroup);
        if (cachedSelection != null && isValidTxgSelection(cachedSelection)) {
            LOGGER.debug("Using cached TXG selection for service group {}: {}",
                    serviceGroup, cachedSelection.getSelectedTxgId());
            return cachedSelection;
        }

        // Need to select new TXG
        TxgInfo selectedTxg = selectTxgForServiceGroup(serviceGroup);
        if (selectedTxg == null) {
            LOGGER.error("Failed to select TXG for service group: {}", serviceGroup);
            return null;
        }

        // Create and cache new selection
        TxgSelectionInfo newSelection = createTxgSelection(serviceGroup, selectedTxg);
        if (newSelection != null) {
            cacheTxgSelection(serviceGroup, newSelection);
            LOGGER.info("Selected TXG {} for service group {} using strategy {}",
                    selectedTxg.getTxgId(), serviceGroup, TXG_LOAD_BALANCE_STRATEGY);
        }

        return newSelection;
    }

    /**
     * Invalidate TXG selection for service group (force re-selection)
     */
    private static void invalidateTxgSelection(String serviceGroup) {
        TxgSelectionInfo selection = TXG_SELECTION_CACHE.remove(serviceGroup);
        if (selection != null) {
            selection.expire();
            LOGGER.info("Invalidated TXG selection for service group: {}", serviceGroup);
        }
    }

    /**
     * Invalidate TXG selection when specific TXG becomes unhealthy
     */
    private static void invalidateTxgSelectionForTxg(String txgId) {
        TXG_SELECTION_CACHE.entrySet().removeIf(entry -> {
            boolean shouldRemove = txgId.equals(entry.getValue().getSelectedTxgId());
            if (shouldRemove) {
                entry.getValue().expire();
                LOGGER.info("Invalidated TXG selection for service group {} due to TXG {} becoming unhealthy",
                        entry.getKey(), txgId);
            }
            return shouldRemove;
        });
    }

    /**
     * Select healthy CG endpoint for communication
     */
    private static String selectHealthyCgEndpoint() {
        List<InetSocketAddress> cgAddresses = getCgAddresses();
        if (CollectionUtils.isEmpty(cgAddresses)) {
            throw new IllegalStateException("No CG addresses available");
        }

        // TODO: Add health checking logic for CG endpoints
        // For now, use simple random selection
        InetSocketAddress selected = cgAddresses.get(ThreadLocalRandom.current().nextInt(cgAddresses.size()));
        return NetUtil.toStringAddress(selected);
    }

    /**
     * Query CG for TXG groups information
     */
    private static String queryCgForTxgs(String serviceGroup) throws RetryableException {
        String cgAddress = selectHealthyCgEndpoint();
        Map<String, String> headers = createCgRequestHeaders();
        Map<String, String> params = new HashMap<>();

        if (StringUtils.isNotBlank(serviceGroup)) {
            params.put("serviceGroup", serviceGroup);
        }

        return executeCgRequest("GET", "http://" + cgAddress + "/metadata/v1/txgroups", params, headers);
    }

    /**
     * Query CG for specific TXG metadata
     */
    private static String queryCgForTxgMetadata(String txgId) throws RetryableException {
        String cgAddress = selectHealthyCgEndpoint();
        Map<String, String> headers = createCgRequestHeaders();
        Map<String, String> params = new HashMap<>();
        params.put("txgId", txgId);

        return executeCgRequest("GET", "http://" + cgAddress + "/metadata/v1/txgroups/metadata", params, headers);
    }

    /**
     * Query CG for TXG leader information
     */
    private static String queryCgForTxgLeader(String txgId) throws RetryableException {
        String cgAddress = selectHealthyCgEndpoint();
        Map<String, String> headers = createCgRequestHeaders();

        return executeCgRequest("GET", "http://" + cgAddress + "/metadata/v1/txgroups/" + txgId + "/leader",
                Collections.emptyMap(), headers);
    }

    /**
     * Watch CG for TXG changes
     */
    private static boolean watchCgForTxgChanges(Map<String, Long> txgTerms) throws RetryableException {
        String cgAddress = selectHealthyCgEndpoint();
        Map<String, String> headers = createCgRequestHeaders();
        headers.put(HTTP.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

        Map<String, Object> watchRequest = new HashMap<>();
        watchRequest.put("txgTerms", txgTerms);

        try {
            String requestBody = OBJECT_MAPPER.writeValueAsString(watchRequest);
            try (CloseableHttpResponse response = HttpClientUtil.doPost(
                    "http://" + cgAddress + "/metadata/v1/watch/txgroups",
                    requestBody, headers, 30000)) {

                if (response != null) {
                    StatusLine statusLine = response.getStatusLine();
                    if (statusLine != null && statusLine.getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
                        handleCgAuthenticationError();
                    }
                    return statusLine != null && statusLine.getStatusCode() == HttpStatus.SC_OK;
                }
            }
        } catch (JsonProcessingException e) {
            LOGGER.error("Failed to serialize watch request", e);
            throw new RetryableException("Failed to serialize watch request", e);
        } catch (IOException e) {
            LOGGER.error("Failed to watch CG for TXG changes: {}", e.getMessage());
            throw new RetryableException("CG watch request failed", e);
        }

        return false;
    }

    /**
     * Create standard headers for CG requests
     */
    private static Map<String, String> createCgRequestHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put(HTTP.CONTENT_TYPE, ContentType.APPLICATION_FORM_URLENCODED.getMimeType());

        // Add authentication if token is available
        if (StringUtils.isNotBlank(jwtToken) && !isTokenExpired()) {
            headers.put(AUTHORIZATION_HEADER, jwtToken);
        }

        return headers;
    }

    /**
     * Execute HTTP request to CG with retry logic
     */
    private static String executeCgRequest(String method, String url, Map<String, String> params,
                                           Map<String, String> headers) throws RetryableException {
        // Refresh token if needed
        if (isTokenExpired()) {
            try {
                String cgAddress = selectHealthyCgEndpoint();
                refreshToken(cgAddress);
                // Update headers with new token
                if (StringUtils.isNotBlank(jwtToken)) {
                    headers.put(AUTHORIZATION_HEADER, jwtToken);
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to refresh token for CG request", e);
            }
        }

        try {
            CloseableHttpResponse response;
            if ("GET".equalsIgnoreCase(method)) {
                response = HttpClientUtil.doGet(url, params, headers, 5000);
            } else if ("POST".equalsIgnoreCase(method)) {
                response = HttpClientUtil.doPost(url, params, headers, 5000);
            } else {
                throw new IllegalArgumentException("Unsupported HTTP method: " + method);
            }

            try {
                if (response != null) {
                    StatusLine statusLine = response.getStatusLine();
                    int statusCode = statusLine.getStatusCode();

                    if (statusCode == HttpStatus.SC_OK) {
                        return EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                    } else if (statusCode == HttpStatus.SC_UNAUTHORIZED) {
                        handleCgAuthenticationError();
                    } else {
                        throw new RetryableException("CG request failed with status: " + statusCode);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            } catch (RetryableException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            throw new RetryableException("CG request failed: " + e.getMessage(), e);
        }

        return null;
    }

    /**
     * Handle CG authentication errors
     */
    private static void handleCgAuthenticationError() throws RetryableException {
        if (StringUtils.isNotBlank(USERNAME) && StringUtils.isNotBlank(PASSWORD)) {
            // Token expired, will be refreshed on next request
            tokenTimeStamp = -1;
            throw new RetryableException("CG authentication failed, token will be refreshed");
        } else {
            throw new AuthenticationFailedException(
                    "CG authentication failed! Configure correct username and password.");
        }
    }

    /**
     * Discover all available TXGs from CG
     */
    private static void discoverTxgsFromCg() throws RetryableException {
        if (!isCgModeEnabled()) {
            return;
        }

        try {
            String response = queryCgForTxgs(null); // Query all TXGs
            if (StringUtils.isNotBlank(response)) {
                parseTxgListResponse(response);
                LOGGER.debug("Successfully discovered TXGs from CG");
            }
        } catch (Exception e) {
            LOGGER.error("Failed to discover TXGs from CG", e);
            throw new RetryableException("TXG discovery failed", e);
        }
    }

    /**
     * Discover TXGs for specific service group
     */
    private static List<String> discoverTxgsForServiceGroup(String serviceGroup) throws RetryableException {
        if (!isCgModeEnabled()) {
            return Collections.emptyList();
        }

        try {
            String response = queryCgForTxgs(serviceGroup);
            if (StringUtils.isNotBlank(response)) {
                return parseTxgListForServiceGroup(response, serviceGroup);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to discover TXGs for service group: {}", serviceGroup, e);
            throw new RetryableException("TXG discovery failed for service group: " + serviceGroup, e);
        }

        return Collections.emptyList();
    }

    /**
     * Get detailed TXG metadata from CG
     */
    private static TxgInfo getTxgMetadataFromCg(String txgId) throws RetryableException {
        if (!isCgModeEnabled()) {
            return null;
        }

        try {
            String response = queryCgForTxgMetadata(txgId);
            if (StringUtils.isNotBlank(response)) {
                return parseTxgMetadataResponse(response, txgId);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to get TXG metadata for: {}", txgId, e);
            throw new RetryableException("Failed to get TXG metadata for: " + txgId, e);
        }

        return null;
    }

    /**
     * Parse TXG list response from CG
     */
    private static void parseTxgListResponse(String response) {
        try {
            JsonNode rootNode = OBJECT_MAPPER.readTree(response);

            if (rootNode.has("txgroups")) {
                JsonNode txgroupsNode = rootNode.get("txgroups");

                if (txgroupsNode.isArray()) {
                    for (JsonNode txgNode : txgroupsNode) {
                        parseSingleTxgInfo(txgNode);
                    }
                }
            }

            // Update cache timestamp
            TXG_CACHE_LAST_UPDATE = System.currentTimeMillis();

        } catch (JsonProcessingException e) {
            LOGGER.error("Failed to parse TXG list response", e);
        }
    }

    /**
     * Parse TXG list for specific service group
     */
    private static List<String> parseTxgListForServiceGroup(String response, String serviceGroup) {
        List<String> txgIds = new ArrayList<>();

        try {
            JsonNode rootNode = OBJECT_MAPPER.readTree(response);

            if (rootNode.has("serviceGroups")) {
                JsonNode serviceGroupsNode = rootNode.get("serviceGroups");
                JsonNode serviceGroupNode = serviceGroupsNode.get(serviceGroup);

                if (serviceGroupNode != null && serviceGroupNode.has("txgroups")) {
                    JsonNode txgroupsNode = serviceGroupNode.get("txgroups");

                    if (txgroupsNode.isArray()) {
                        for (JsonNode txgIdNode : txgroupsNode) {
                            String txgId = txgIdNode.asText();
                            if (StringUtils.isNotBlank(txgId)) {
                                txgIds.add(txgId);
                            }
                        }
                    }
                }
            }

            // Update mapping
            updateServiceGroupTxgMapping(serviceGroup, txgIds);

        } catch (JsonProcessingException e) {
            LOGGER.error("Failed to parse TXG list for service group: {}", serviceGroup, e);
        }

        return txgIds;
    }

    /**
     * Parse single TXG information from JSON
     */
    private static void parseSingleTxgInfo(JsonNode txgNode) {
        try {
            String txgId = txgNode.get("txgId").asText();
            String serviceGroup = txgNode.has("serviceGroup") ? txgNode.get("serviceGroup").asText() : "";

            TxgInfo txgInfo = new TxgInfo(txgId, serviceGroup);

            // Parse leader information
            if (txgNode.has("leader")) {
                JsonNode leaderNode = txgNode.get("leader");
                Node leader = parseNodeFromJson(leaderNode);
                txgInfo.setLeader(leader);
            }

            // Parse followers
            if (txgNode.has("followers") && txgNode.get("followers").isArray()) {
                List<Node> followers = new ArrayList<>();
                for (JsonNode followerNode : txgNode.get("followers")) {
                    Node follower = parseNodeFromJson(followerNode);
                    followers.add(follower);
                }
                txgInfo.setFollowers(followers);
            }

            // Parse other fields
            if (txgNode.has("term")) {
                txgInfo.setTerm(txgNode.get("term").asLong());
            }

            if (txgNode.has("healthy")) {
                txgInfo.setHealthy(txgNode.get("healthy").asBoolean());
            }

            if (txgNode.has("weight")) {
                txgInfo.setWeight(txgNode.get("weight").asInt());
            }

            // Cache the TXG info
            cacheTxgInfo(txgId, txgInfo);

        } catch (Exception e) {
            LOGGER.error("Failed to parse TXG info from JSON", e);
        }
    }

    /**
     * Parse TXG metadata response
     */
    private static TxgInfo parseTxgMetadataResponse(String response, String txgId) {
        try {
            JsonNode rootNode = OBJECT_MAPPER.readTree(response);

            if (rootNode.has("txgroup")) {
                JsonNode txgNode = rootNode.get("txgroup");
                parseSingleTxgInfo(txgNode);
                return getCachedTxgInfo(txgId);
            }

        } catch (JsonProcessingException e) {
            LOGGER.error("Failed to parse TXG metadata response for: {}", txgId, e);
        }

        return null;
    }

    /**
     * Parse Node from JSON (helper method)
     */
    private static Node parseNodeFromJson(JsonNode nodeJson) {
        Node node = new Node();

        // Parse transaction endpoint
        if (nodeJson.has("transaction")) {
            JsonNode transactionNode = nodeJson.get("transaction");
            String host = transactionNode.get("host").asText();
            int port = transactionNode.get("port").asInt();
            node.setTransaction(node.createEndpoint(host, port, "transaction"));
        }

        // Parse control endpoint
        if (nodeJson.has("control")) {
            JsonNode controlNode = nodeJson.get("control");
            String host = controlNode.get("host").asText();
            int port = controlNode.get("port").asInt();
            node.setControl(node.createEndpoint(host, port, "control"));
        }

        // Parse internal endpoint
        if (nodeJson.has("internal")) {
            JsonNode internalNode = nodeJson.get("internal");
            String host = internalNode.get("host").asText();
            int port = internalNode.get("port").asInt();
            node.setInternal(node.createEndpoint(host, port, "raft"));
        }

        // Parse other fields
        if (nodeJson.has("group")) {
            node.setGroup(nodeJson.get("group").asText());
        }

        if (nodeJson.has("version")) {
            node.setVersion(nodeJson.get("version").asText());
        }

        return node;
    }

    /**
     * Refresh TXG cache from CG
     */
    private static void refreshTxgCacheFromCg() {
        if (!isCgModeEnabled() || TXG_CACHE_INITIALIZING.get()) {
            return;
        }

        if (!TXG_CACHE_INITIALIZING.compareAndSet(false, true)) {
            return; // Another thread is already refreshing
        }

        try {
            LOGGER.debug("Refreshing TXG cache from CG");

            // Discover all TXGs
            discoverTxgsFromCg();

            // Get detailed metadata for each cached TXG
            Set<String> txgIds = new HashSet<>(TXG_CACHE.keySet());
            for (String txgId : txgIds) {
                try {
                    TxgInfo detailedInfo = getTxgMetadataFromCg(txgId);
                    if (detailedInfo != null) {
                        cacheTxgInfo(txgId, detailedInfo);
                    }
                } catch (Exception e) {
                    LOGGER.warn("Failed to refresh metadata for TXG: {}", txgId, e);
                    // Mark TXG as potentially unhealthy
                    TxgInfo cachedInfo = TXG_CACHE.get(txgId);
                    if (cachedInfo != null) {
                        cachedInfo.setHealthy(false);
                    }
                }
            }

            TXG_CACHE_LAST_UPDATE = System.currentTimeMillis();
            LOGGER.info("TXG cache refreshed successfully. Cached TXGs: {}", TXG_CACHE.size());

        } catch (Exception e) {
            LOGGER.error("Failed to refresh TXG cache from CG", e);
        } finally {
            TXG_CACHE_INITIALIZING.set(false);
        }
    }

    /**
     * Initialize TXG cache for specific service group
     */
    private static void initializeTxgCacheForServiceGroup(String serviceGroup) {
        if (!isCgModeEnabled()) {
            return;
        }

        try {
            LOGGER.debug("Initializing TXG cache for service group: {}", serviceGroup);

            // Discover TXGs for this service group
            List<String> txgIds = discoverTxgsForServiceGroup(serviceGroup);

            if (CollectionUtils.isNotEmpty(txgIds)) {
                // Get detailed metadata for each TXG
                for (String txgId : txgIds) {
                    try {
                        TxgInfo txgInfo = getTxgMetadataFromCg(txgId);
                        if (txgInfo != null) {
                            txgInfo.setServiceGroup(serviceGroup); // Ensure service group is set
                            cacheTxgInfo(txgId, txgInfo);
                        }
                    } catch (Exception e) {
                        LOGGER.warn("Failed to get metadata for TXG: {}", txgId, e);
                    }
                }

                LOGGER.info("Initialized TXG cache for service group {}: {} TXGs",
                        serviceGroup, txgIds.size());
            } else {
                LOGGER.warn("No TXGs found for service group: {}", serviceGroup);
            }

        } catch (Exception e) {
            LOGGER.error("Failed to initialize TXG cache for service group: {}", serviceGroup, e);
        }
    }

    /**
     * Get TXG endpoints for service group from cache or CG
     */
    private static List<InetSocketAddress> getTxgEndpointsForServiceGroup(String serviceGroup) {
        // Check if we have a cached selection
        TxgSelectionInfo selection = getOrSelectTxg(serviceGroup);
        if (selection != null && CollectionUtils.isNotEmpty(selection.getTransactionEndpoints())) {
            LOGGER.debug("Using cached TXG selection for service group {}: {}",
                    serviceGroup, selection.getSelectedTxgId());
            return selection.getTransactionEndpoints();
        }

        // No valid selection, try to initialize cache for this service group
        try {
            initializeTxgCacheForServiceGroup(serviceGroup);

            // Try selection again after cache initialization
            selection = getOrSelectTxg(serviceGroup);
            if (selection != null && CollectionUtils.isNotEmpty(selection.getTransactionEndpoints())) {
                return selection.getTransactionEndpoints();
            }

        } catch (Exception e) {
            LOGGER.error("Failed to get TXG endpoints for service group: {}", serviceGroup, e);
        }

        LOGGER.warn("No TXG endpoints available for service group: {}", serviceGroup);
        return Collections.emptyList();
    }

    /**
     * Validate and refresh cached TXG info if stale
     */
    private static TxgInfo getValidatedTxgInfo(String txgId) {
        TxgInfo cachedInfo = getCachedTxgInfo(txgId);

        if (cachedInfo == null) {
            // Try to fetch from CG
            try {
                cachedInfo = getTxgMetadataFromCg(txgId);
                if (cachedInfo != null) {
                    cacheTxgInfo(txgId, cachedInfo);
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to fetch TXG metadata for: {}", txgId, e);
            }
        }

        return cachedInfo;
    }

    /**
     * Get cache statistics for monitoring
     */
    private static Map<String, Object> getTxgCacheStatistics() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalTxgs", TXG_CACHE.size());
        stats.put("healthyTxgs", TXG_CACHE.values().stream().mapToLong(txg -> txg.isHealthy() ? 1 : 0).sum());
        stats.put("cachedSelections", TXG_SELECTION_CACHE.size());
        stats.put("serviceGroupMappings", SERVICE_GROUP_TXG_MAPPING.size());
        stats.put("lastCacheUpdate", TXG_CACHE_LAST_UPDATE);
        stats.put("cacheAge", System.currentTimeMillis() - TXG_CACHE_LAST_UPDATE);

        return stats;
    }

    /**
     * TXG node pushes metadata changes to CG
     * This method should be called by TXG nodes when their metadata changes
     */
    public static void pushTxgMetadataChangeToCg(String txgId, RaftClusterMetadata newMetadata) {
        if (!isCgModeEnabled()) {
            return;
        }

        try {
            LOGGER.info("Pushing TXG metadata change to CG: {} (term: {})", txgId, newMetadata.getTerm());

            String cgAddress = selectHealthyCgEndpoint();
            Map<String, String> headers = createCgRequestHeaders();
            headers.put(HTTP.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

            // Create the metadata update request
            Map<String, Object> updateRequest = new HashMap<>();
            updateRequest.put("txgId", txgId);
            updateRequest.put("term", newMetadata.getTerm());
            updateRequest.put("timestamp", System.currentTimeMillis());

            // Add leader information
            if (newMetadata.getLeader() != null) {
                updateRequest.put("leader", nodeToJsonMap(newMetadata.getLeader()));
            }

            // Add follower information
            if (newMetadata.getFollowers() != null) {
                List<Map<String, Object>> followers = newMetadata.getFollowers().stream()
                        .map(RaftRegistryServiceImpl::nodeToJsonMap)
                        .collect(Collectors.toList());
                updateRequest.put("followers", followers);
            }

            // Add learner information
            if (newMetadata.getLearner() != null) {
                List<Map<String, Object>> learners = newMetadata.getLearner().stream()
                        .map(RaftRegistryServiceImpl::nodeToJsonMap)
                        .collect(Collectors.toList());
                updateRequest.put("learners", learners);
            }

            String requestBody = OBJECT_MAPPER.writeValueAsString(updateRequest);

            try (CloseableHttpResponse response = HttpClientUtil.doPost(
                    "http://" + cgAddress + "/metadata/v1/txgroups/update",
                    requestBody, headers, 5000)) {

                if (response != null) {
                    int statusCode = response.getStatusLine().getStatusCode();
                    if (statusCode == HttpStatus.SC_OK) {
                        LOGGER.info("Successfully pushed TXG metadata change to CG: {}", txgId);
                    } else {
                        LOGGER.warn("Failed to push TXG metadata to CG: {} (status: {})", txgId, statusCode);
                    }
                }
            }

        } catch (Exception e) {
            LOGGER.error("Failed to push TXG metadata change to CG: {}", txgId, e);
        }
    }

    /**
     * Convert Node to JSON-compatible map
     */
    private static Map<String, Object> nodeToJsonMap(Node node) {
        Map<String, Object> nodeMap = new HashMap<>();

        if (node.getTransaction() != null) {
            Map<String, Object> transaction = new HashMap<>();
            transaction.put("host", node.getTransaction().getHost());
            transaction.put("port", node.getTransaction().getPort());
            nodeMap.put("transaction", transaction);
        }

        if (node.getControl() != null) {
            Map<String, Object> control = new HashMap<>();
            control.put("host", node.getControl().getHost());
            control.put("port", node.getControl().getPort());
            nodeMap.put("control", control);
        }

        if (node.getInternal() != null) {
            Map<String, Object> internal = new HashMap<>();
            internal.put("host", node.getInternal().getHost());
            internal.put("port", node.getInternal().getPort());
            nodeMap.put("internal", internal);
        }

        if (node.getGroup() != null) {
            nodeMap.put("group", node.getGroup());
        }

        if (node.getVersion() != null) {
            nodeMap.put("version", node.getVersion());
        }

        if (node.getMetadata() != null) {
            nodeMap.put("metadata", node.getMetadata());
        }

        return nodeMap;
    }

    /**
     * Enhanced TXG metadata change handler with client notification
     */
    private static void handleTxgMetadataChange(String txgId, long newTerm) {
        LOGGER.info("Processing TXG metadata change: {} (term: {})", txgId, newTerm);

        try {
            // Check if we have this TXG cached
            TxgInfo cachedInfo = getCachedTxgInfo(txgId);

            if (cachedInfo != null && cachedInfo.getTerm() >= newTerm) {
                LOGGER.debug("Ignoring stale TXG metadata change: {} (cached_term: {}, new_term: {})",
                        txgId, cachedInfo.getTerm(), newTerm);
                return;
            }

            // Fetch updated metadata from CG
            TxgInfo updatedInfo = getTxgMetadataFromCg(txgId);

            if (updatedInfo != null) {
                // Update cache
                cacheTxgInfo(txgId, updatedInfo);

                // Check if this TXG is selected by any service groups
                List<String> affectedServiceGroups = findServiceGroupsUsingTxg(txgId);

                for (String serviceGroup : affectedServiceGroups) {
                    TxgSelectionInfo selection = getTxgSelection(serviceGroup);

                    if (selection != null && txgId.equals(selection.getSelectedTxgId())) {
                        if (updatedInfo.isReady()) {
                            // TXG is still healthy, update endpoints
                            List<InetSocketAddress> newEndpoints = Collections.singletonList(
                                    updatedInfo.getLeaderTransactionEndpoint());

                            selection.setTransactionEndpoints(newEndpoints);
                            cacheTxgSelection(serviceGroup, selection);

                            LOGGER.info("Updated endpoints for service group {} after TXG change: {}",
                                    serviceGroup, newEndpoints);
                        } else {
                            // TXG is not ready, invalidate selection
                            invalidateTxgSelection(serviceGroup);

                            LOGGER.warn("Invalidated selection for service group {} due to unhealthy TXG: {}",
                                    serviceGroup, txgId);
                        }
                    }
                }

                // Notify any local listeners (if needed)
                notifyLocalTxgChangeListeners(txgId, newTerm, updatedInfo);

            } else {
                LOGGER.error("Failed to fetch updated TXG metadata from CG: {}", txgId);

                // Mark cached TXG as potentially unhealthy
                if (cachedInfo != null) {
                    cachedInfo.setHealthy(false);
                    updateTxgHealthStatus(txgId, false);
                    invalidateTxgSelectionForTxg(txgId);
                }
            }

        } catch (Exception e) {
            LOGGER.error("Error handling TXG metadata change: {}", txgId, e);
        }
    }

    /**
     * Find service groups that are currently using the specified TXG
     */
    private static List<String> findServiceGroupsUsingTxg(String txgId) {
        return TXG_SELECTION_CACHE.entrySet().stream()
                .filter(entry -> {
                    TxgSelectionInfo selection = entry.getValue();
                    return selection != null &&
                            !selection.isExpired() &&
                            txgId.equals(selection.getSelectedTxgId());
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    /**
     * Notify local TXG change listeners
     */
    private static void notifyLocalTxgChangeListeners(String txgId, long newTerm, TxgInfo updatedInfo) {
        // This is a hook for any local listeners that need to be notified of TXG changes
        // For example, connection pools, caches, etc.

        LOGGER.debug("Notifying local listeners of TXG change: {} (term: {})", txgId, newTerm);

        // Example: Update connection health status
        updateTxgHealthStatus(txgId, updatedInfo.isHealthy());

        // Example: Log metrics or trigger monitoring alerts
        if (!updatedInfo.isHealthy()) {
            LOGGER.warn("TXG {} is now unhealthy after metadata change", txgId);
        }
    }
}
