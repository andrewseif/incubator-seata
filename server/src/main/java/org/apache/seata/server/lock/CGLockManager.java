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

package org.apache.seata.server.lock;

import org.apache.seata.server.lock.dto.CGLockRequest;
import org.apache.seata.server.lock.dto.LockResult;

import java.util.concurrent.CompletableFuture;

/**
 * Cluster Group Lock Manager Interface for multi-raft distributed lock management.
 * Provides distributed lock operations across multiple raft groups.
 * LOCK_MAP: resourceId -> instance -> bucketId -> BucketLockMap
 */
public interface CGLockManager {

    /**
     * Acquire locks for the specified rows
     *
     * @param request the lock acquisition request
     * @return CompletableFuture containing lock result
     */
    CompletableFuture<LockResult> acquireLock(CGLockRequest request);

    /**
     * Release locks for the specified rows
     *
     * @param request the lock release request
     * @return CompletableFuture containing release result
     */
    CompletableFuture<LockResult> releaseLock(CGLockRequest request);

    /**
     * Check if the specified locks are acquirable without actually acquiring them
     *
     * @param request the lock query request
     * @return CompletableFuture containing lockable status
     */
    CompletableFuture<LockResult> isLockable(CGLockRequest request);

    /**
     * Clean all locks - used when follower falls behind and needs to apply leader's snapshot
     *
     * @return CompletableFuture containing clean result
     */
    CompletableFuture<LockResult> cleanAllLocks();
}
