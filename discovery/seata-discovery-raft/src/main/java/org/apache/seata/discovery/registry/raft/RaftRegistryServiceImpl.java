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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.http.HttpStatus;
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
    private static final String PRO_CG_SERVER_ADDR_KEY = "cgServerAddr";
    private static final String PRO_USERNAME_KEY = "username";
    private static final String PRO_PASSWORD_KEY = "password";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String TOKEN_VALID_TIME_MS_KEY = "tokenValidityInMilliseconds";
    private static final String META_DATA_MAX_AGE_MS = "metadataMaxAgeMs";
    private static final String IP_PORT_SPLIT_CHAR = ":";
    private static final String PREFERRED_NETWORKS;

    private static final Map<String, InetSocketAddress> TXG_LEADERS = new ConcurrentHashMap<>();

    // CG addresses
    private static final List<InetSocketAddress> CG_ADDRESSES = new ArrayList<>();

    // Configuration
    private static final Configuration CONFIG = ConfigurationFactory.CURRENT_FILE_INSTANCE;
    private static final long TOKEN_EXPIRE_TIME_IN_MILLISECONDS;
    private static final String USERNAME;
    private static final String PASSWORD;
    private static final long REFRESH_INTERVAL = 30000L; // 30 seconds

    // Traditional mode fields (existing)
    private static final Map<String, List<InetSocketAddress>> INIT_ADDRESSES = new HashMap<>();
    private static final Metadata METADATA = new Metadata();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static volatile String CURRENT_TRANSACTION_SERVICE_GROUP;
    private static volatile String CURRENT_TRANSACTION_CLUSTER_NAME;
    private static volatile ThreadPoolExecutor REFRESH_METADATA_EXECUTOR;
    private static final AtomicBoolean CLOSED = new AtomicBoolean(false);
    private static final Map<String, List<InetSocketAddress>> ALIVE_NODES = new ConcurrentHashMap<>();

    // Background refresh
    private static volatile ScheduledExecutorService REFRESH_EXECUTOR;
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);

    // Token management
    public static String jwtToken;
    private static long tokenTimeStamp = -1;

    private static volatile RaftRegistryServiceImpl instance;


    static {
        TOKEN_EXPIRE_TIME_IN_MILLISECONDS = CONFIG.getLong(getTokenExpireTimeInMillisecondsKey(), 29 * 60 * 1000L);
        USERNAME = CONFIG.getConfig(getRaftUserNameKey());
        PASSWORD = CONFIG.getConfig(getRaftPassWordKey());
        PREFERRED_NETWORKS = CONFIG.getConfig(getPreferredNetworks());

        initializeCgMode();
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

    /**
     * Initialize CG mode - startup fetch all TXGs and cache them locally
     */
    private static void initializeCgMode() {
        String cgAddresses = CONFIG.getConfig(getCgAddrFileKey());
        if (StringUtils.isBlank(cgAddresses)) {
            LOGGER.info("No CG addresses configured, TXG load balancing disabled");
            return;
        }

        // Parse CG addresses
        for (String addr : cgAddresses.split(",")) {
            String[] parts = addr.trim().split(":");
            if (parts.length == 2) {
                try {
                    CG_ADDRESSES.add(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])));
                } catch (NumberFormatException e) {
                    LOGGER.warn("Invalid CG address: {}", addr);
                }
            }
        }

        if (!CG_ADDRESSES.isEmpty()) {
            startBackgroundRefresh();
            LOGGER.info("TXG load balancing enabled with {} CG addresses", CG_ADDRESSES.size());
        }
    }

    /**
     * Start background refresh with watch events and a timeout to refresh the TXG cache
     */
    private static void startBackgroundRefresh() {
        if (REFRESH_EXECUTOR == null) {
            synchronized (RaftRegistryServiceImpl.class) {
                if (REFRESH_EXECUTOR == null) {
                    REFRESH_EXECUTOR = Executors.newSingleThreadScheduledExecutor(
                            new NamedThreadFactory("txg-refresh", true));

                    REFRESH_EXECUTOR.scheduleWithFixedDelay(() -> {
                        if (!CLOSED.get()) {
                            refreshTxgCache();
                        }
                    }, 0, REFRESH_INTERVAL, TimeUnit.MILLISECONDS);

                    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        CLOSED.set(true);
                        if (REFRESH_EXECUTOR != null) {
                            REFRESH_EXECUTOR.shutdown();
                        }
                    }));
                }
            }
        }
    }

    /**
     * Refresh TXG cache from CG
     */
    private static void refreshTxgCache() {
        if (CG_ADDRESSES.isEmpty()) {
            return;
        }

        try {
            // Try each CG address until one works
            for (InetSocketAddress cgAddr : CG_ADDRESSES) {
                try {
                    Map<String, InetSocketAddress> newTxgLeaders = fetchTxgLeadersFromCg(cgAddr);
                    if (!newTxgLeaders.isEmpty()) {
                        TXG_LEADERS.clear();
                        TXG_LEADERS.putAll(newTxgLeaders);
                        LOGGER.debug("Refreshed TXG cache with {} leaders from CG: {}",
                                newTxgLeaders.size(), cgAddr);
                        return;
                    }
                } catch (Exception e) {
                    LOGGER.debug("Failed to fetch from CG {}: {}", cgAddr, e.getMessage());
                }
            }
            LOGGER.warn("Failed to refresh TXG cache from any CG address");
        } catch (Exception e) {
            LOGGER.error("Error refreshing TXG cache", e);
        }
    }

    /**
     * Fetch TXG leaders from specific CG
     */
    private static Map<String, InetSocketAddress> fetchTxgLeadersFromCg(InetSocketAddress cgAddr) throws Exception {
        Map<String, InetSocketAddress> leaders = new HashMap<>();

        // Simple HTTP request to CG
        String url = "http://" + cgAddr.getHostString() + ":" + cgAddr.getPort() + "/metadata/v1/txgroups";

        try (CloseableHttpResponse response = HttpClientUtil.doGet(url, null, null, 5000)) {
            if (response != null && response.getStatusLine().getStatusCode() == 200) {
                String json = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);

                // Parse JSON response
                JsonNode root = OBJECT_MAPPER.readTree(json);
                if (root.has("txgroups")) {
                    JsonNode txgroups = root.get("txgroups");
                    for (JsonNode txg : txgroups) {
                        String txgId = txg.get("txgId").asText();
                        if (txg.has("leader") && txg.get("leader").has("transaction")) {
                            JsonNode leader = txg.get("leader").get("transaction");
                            String host = leader.get("host").asText();
                            int port = leader.get("port").asInt();
                            leaders.put(txgId, new InetSocketAddress(host, port));
                        }
                    }
                }
            }
        }

        return leaders;
    }

    /**
     * Get TXG leader (used by TxgLoadBalance)
     */
    public static InetSocketAddress getTxgLeader(String txgId) {
        return TXG_LEADERS.get(txgId);
    }

    /**
     * Get all TXG leaders (used by aliveLookup)
     */
    public static Map<String, InetSocketAddress> getAllTxgLeaders() {
        return new HashMap<>(TXG_LEADERS);
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

    @Override
    public List<InetSocketAddress> aliveLookup(String transactionServiceGroup) {
        Map<String, InetSocketAddress> allLeaders = getAllTxgLeaders();

        if (allLeaders.isEmpty()) {
            // Fall back to traditional mode
            return aliveLookupTraditional(transactionServiceGroup);
        }

        // Return all TXG leaders
        return new ArrayList<>(allLeaders.values());
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
    public List<InetSocketAddress> refreshAliveLookup(String transactionServiceGroup, List<InetSocketAddress> aliveAddress) {
        // Check if CG mode is available
        if (!CG_ADDRESSES.isEmpty()) {
            // Simple implementation - just return alive addresses that match TXG leaders
            Map<String, InetSocketAddress> allLeaders = getAllTxgLeaders();
            List<InetSocketAddress> filteredAddresses = aliveAddress.stream()
                    .filter(addr -> allLeaders.containsValue(addr))
                    .collect(Collectors.toList());

            ALIVE_NODES.put(transactionServiceGroup, filteredAddresses);
            return filteredAddresses;
        } else {
            // Traditional mode
            return refreshAliveLookupTraditional(transactionServiceGroup, aliveAddress);
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

        // Check if CG mode is available
        if (!CG_ADDRESSES.isEmpty()) {
            // Return all TXG leaders - let LoadBalance handle selection
            Map<String, InetSocketAddress> allLeaders = getAllTxgLeaders();
            if (!allLeaders.isEmpty()) {
                return new ArrayList<>(allLeaders.values());
            }
        }

        // Fall back to traditional single-raft discovery
        return lookupTraditional(key, clusterName);
    }

    @Override
    public void close() {
        CLOSED.compareAndSet(false, true);
        if (REFRESH_EXECUTOR != null) {
            REFRESH_EXECUTOR.shutdown();
        }
        if (REFRESH_METADATA_EXECUTOR != null) {
            REFRESH_METADATA_EXECUTOR.shutdown();
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

    private static String getMetadataMaxAgeMs() {
        return String.join(
                ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR,
                ConfigurationKeys.FILE_ROOT_REGISTRY,
                REGISTRY_TYPE,
                META_DATA_MAX_AGE_MS);
    }

}
