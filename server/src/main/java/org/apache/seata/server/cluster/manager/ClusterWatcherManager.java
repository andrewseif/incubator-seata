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
package org.apache.seata.server.cluster.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.stream.Collectors;
import org.apache.seata.common.rpc.http.HttpContext;
import org.apache.seata.common.thread.NamedThreadFactory;
import org.apache.seata.server.cluster.listener.ClusterChangeEvent;
import org.apache.seata.server.cluster.listener.ClusterChangeListener;
import org.apache.seata.server.cluster.listener.TxgChangeEvent;
import org.apache.seata.server.cluster.watch.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Component
public class ClusterWatcherManager implements ClusterChangeListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final Map<String, Queue<Watcher<HttpContext>>> WATCHERS = new ConcurrentHashMap<>();

    private static final Map<String, Long> GROUP_UPDATE_TIME = new ConcurrentHashMap<>();

    private static final Map<String, Queue<Watcher<HttpContext>>> TXG_WATCHERS = new ConcurrentHashMap<>();

    private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor =
            new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("long-polling", 1));

    @Override
    @EventListener
    @Async
    public void onChangeEvent(ClusterChangeEvent event) {
        if (event.getTerm() > 0) {
            GROUP_UPDATE_TIME.put(event.getGroup(), event.getTerm());
            // Notifications are made of changes in cluster information
            Optional.ofNullable(WATCHERS.remove(event.getGroup()))
                    .ifPresent(watchers -> watchers.parallelStream().forEach(this::notifyWatcher));
        }
    }

    private void notifyWatcher(Watcher<HttpContext> watcher) {
        watcher.setDone(true);
        sendWatcherResponse(watcher, HttpResponseStatus.OK);
    }

    private void sendWatcherResponse(Watcher<HttpContext> watcher, HttpResponseStatus nettyStatus) {
        HttpContext context = watcher.getAsyncContext();
        if (!(context instanceof HttpContext)) {
            logger.warn(
                    "Unsupported context type for watcher on group {}: {}",
                    watcher.getGroup(),
                    context != null ? context.getClass().getName() : "null");
            return;
        }
        ChannelHandlerContext ctx = context.getContext();
        if (!context.isHttp2()) {
            if (ctx.channel().isActive()) {
                HttpResponse response =
                        new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, nettyStatus, Unpooled.EMPTY_BUFFER);
                response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);

                if (!context.isKeepAlive()) {
                    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
                } else {
                    ctx.writeAndFlush(response);
                }
            } else {
                logger.warn(
                        "Netty channel is not active for watcher on group {}, cannot send response.",
                        watcher.getGroup());
            }
        }
    }

    public void registryWatcher(Watcher<HttpContext> watcher) {
        String group = watcher.getGroup();
        Long term = GROUP_UPDATE_TIME.get(group);
        if (term == null || watcher.getTerm() >= term) {
            WATCHERS.computeIfAbsent(group, value -> new ConcurrentLinkedQueue<>())
                    .add(watcher);
        } else {
            notifyWatcher(watcher);
        }
    }

    /**
     * Register watcher for TXG changes
     */
    public void registryTxgWatcher(Watcher<HttpContext> watcher) {
        String txgId = watcher.getGroup(); // Using group field to store TXG ID

        // Check if TXG metadata has changed since client's last known term
        // This would need to be implemented based on your TXG change tracking

        TXG_WATCHERS.computeIfAbsent(txgId, key -> new ConcurrentLinkedQueue<>())
                .add(watcher);

        logger.debug("Registered TXG watcher for TXG: {}", txgId);
    }

    /**
     * Notify watchers of TXG changes
     */
    public void notifyTxgWatchers(String txgId, long newTerm) {
        Queue<Watcher<HttpContext>> watchers = TXG_WATCHERS.remove(txgId);
        if (watchers != null) {
            watchers.parallelStream().forEach(watcher -> {
                if (newTerm > watcher.getTerm()) {
                    notifyTxgWatcher(watcher, txgId, newTerm);
                }
            });
            logger.debug("Notified {} watchers for TXG: {} (term: {})", watchers.size(), txgId, newTerm);
        }
    }

    private void notifyTxgWatcher(Watcher<HttpContext> watcher, String txgId, long newTerm) {
        watcher.setDone(true);

        // Prepare response data
        Map<String, Object> response = new HashMap<>();
        response.put("txgId", txgId);
        response.put("newTerm", newTerm);
        response.put("changed", true);
        response.put("timestamp", System.currentTimeMillis());

        sendTxgWatcherResponse(watcher, HttpResponseStatus.OK, response);
    }

    private void sendTxgWatcherResponse(Watcher<HttpContext> watcher, HttpResponseStatus status, Object responseData) {
        HttpContext context = watcher.getAsyncContext();
        if (!(context instanceof HttpContext)) {
            logger.warn("Unsupported context type for TXG watcher: {}",
                    context != null ? context.getClass().getName() : "null");
            return;
        }

        try {
            ChannelHandlerContext ctx = context.getContext();
            if (ctx.channel().isActive()) {
                String jsonResponse = new ObjectMapper().writeValueAsString(responseData);
                ByteBuf content = Unpooled.copiedBuffer(jsonResponse, StandardCharsets.UTF_8);

                HttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
                response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
                response.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
                if (!context.isKeepAlive()) {
                    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
                } else {
                    ctx.writeAndFlush(response);
                }
            } else {
                logger.warn("Netty channel is not active for TXG watcher: {}", watcher.getGroup());
            }
        } catch (Exception e) {
            logger.error("Failed to send TXG watcher response", e);
        }
    }

    /**
     * Get count of active watchers (for monitoring)
     */
    public int getActiveWatcherCount() {
        int totalWatchers = WATCHERS.values().stream()
                .mapToInt(Queue::size)
                .sum();
        int totalTxgWatchers = TXG_WATCHERS.values().stream()
                .mapToInt(Queue::size)
                .sum();
        return totalWatchers + totalTxgWatchers;
    }

    /**
     * Get TXG watcher statistics
     */
    public Map<String, Object> getTxgWatcherStatistics() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("activeTxgWatchers", TXG_WATCHERS.values().stream().mapToInt(Queue::size).sum());
        stats.put("watchedTxgCount", TXG_WATCHERS.size());
        stats.put("txgWatchersByTxg", TXG_WATCHERS.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().size())));
        return stats;
    }

    // Updated the existing init() method to handle TXG watchers cleanup
    @PostConstruct
    public void init() {
        // Responds to monitors that time out
        scheduledThreadPoolExecutor.scheduleAtFixedRate(
                () -> {
                    // Handle regular cluster watchers
                    for (String group : WATCHERS.keySet()) {
                        Optional.ofNullable(WATCHERS.remove(group))
                                .ifPresent(watchers -> watchers.parallelStream().forEach(watcher -> {
                                    if (System.currentTimeMillis() >= watcher.getTimeout()) {
                                        watcher.setDone(true);
                                        sendWatcherResponse(watcher, HttpResponseStatus.NOT_MODIFIED);
                                    }
                                    if (!watcher.isDone()) {
                                        // Re-register
                                        registryWatcher(watcher);
                                    }
                                }));
                    }

                    // Handle TXG watchers
                    for (String txgId : TXG_WATCHERS.keySet()) {
                        Optional.ofNullable(TXG_WATCHERS.remove(txgId))
                                .ifPresent(watchers -> watchers.parallelStream().forEach(watcher -> {
                                    if (System.currentTimeMillis() >= watcher.getTimeout()) {
                                        watcher.setDone(true);
                                        sendTxgWatcherResponse(watcher, HttpResponseStatus.NOT_MODIFIED,
                                                Collections.singletonMap("timeout", true));
                                    }
                                    if (!watcher.isDone()) {
                                        // Re-register
                                        registryTxgWatcher(watcher);
                                    }
                                }));
                    }
                },
                1,
                1,
                TimeUnit.SECONDS);
    }

    /**
     * Handle TXG change events
     */
    @EventListener
    @Async
    public void onTxgChangeEvent(TxgChangeEvent event) {
        logger.info("Received TXG change event: {}", event);

        try {
            // Notify all watchers for this TXG
            notifyTxgWatchers(event.getTxgId(), event.getTerm());

            // Update group update time for this TXG
            GROUP_UPDATE_TIME.put(event.getTxgId(), event.getTerm());

        } catch (Exception e) {
            logger.error("Failed to handle TXG change event: {}", event, e);
        }
    }
}