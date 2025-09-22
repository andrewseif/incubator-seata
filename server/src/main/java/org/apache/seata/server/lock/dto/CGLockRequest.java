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

package org.apache.seata.server.lock.dto;

import org.apache.seata.core.lock.RowLock;

import java.util.List;

/**
 * CG Lock Request DTO for HTTP communication
 */
public class CGLockRequest {

    private String requestId;
    private String parent;
    private String owner;
    private String instance;
    private List<RowLock> rowLocks;
    private boolean autoCommit = true;
    private boolean skipCheckLock = false;
    private long timeout = 30000; // 30 seconds default timeout

    public CGLockRequest() {}

    public CGLockRequest(String parent, String owner, List<RowLock> rowLocks) {
        this.parent = parent;
        this.owner = owner;
        this.rowLocks = rowLocks;
        this.requestId = generateRequestId();
    }

    private String generateRequestId() {
        return parent + "_" + owner + "_" + System.currentTimeMillis();
    }

    // Getters and Setters
    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getParent() {
        return parent;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public List<RowLock> getRowLocks() {
        return rowLocks;
    }

    public void setRowLocks(List<RowLock> rowLocks) {
        this.rowLocks = rowLocks;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public boolean isSkipCheckLock() {
        return skipCheckLock;
    }

    public void setSkipCheckLock(boolean skipCheckLock) {
        this.skipCheckLock = skipCheckLock;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }
}
