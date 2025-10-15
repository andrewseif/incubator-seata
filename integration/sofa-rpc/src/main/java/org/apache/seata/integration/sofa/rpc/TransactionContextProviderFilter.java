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
package org.apache.seata.integration.sofa.rpc;

import com.alipay.sofa.rpc.context.RpcInternalContext;
import com.alipay.sofa.rpc.core.exception.SofaRpcException;
import com.alipay.sofa.rpc.core.request.SofaRequest;
import com.alipay.sofa.rpc.core.response.SofaResponse;
import com.alipay.sofa.rpc.ext.Extension;
import com.alipay.sofa.rpc.filter.AutoActive;
import com.alipay.sofa.rpc.filter.Filter;
import com.alipay.sofa.rpc.filter.FilterInvoker;
import org.apache.seata.common.util.StringUtils;
import org.apache.seata.core.context.RootContext;
import org.apache.seata.core.model.BranchType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TransactionContext on provider side.
 *
 * @since 0.6.0
 */
@Extension(value = "transactionContextProvider")
@AutoActive(providerSide = true)
public class TransactionContextProviderFilter extends Filter {

    /**
     * Logger for this class
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionContextProviderFilter.class);

    @Override
    public SofaResponse invoke(FilterInvoker filterInvoker, SofaRequest sofaRequest) throws SofaRpcException {
        String xid = RootContext.getXID();
        String rpcXid = getRpcXid(sofaRequest);
        BranchType branchType = RootContext.getBranchType();
        String rpcBranchType = getBranchType(sofaRequest);
        String txg = RootContext.getTXG();
        String rpcTxg = getTxg(sofaRequest);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "context in RootContext[{},{},{}], context in RpcContext[{},{},{}]",
                    xid,
                    branchType,
                    txg,
                    rpcXid,
                    rpcBranchType,
                    rpcTxg);
        }
        boolean bind = false;
        if (xid != null) {
            RpcInternalContext.getContext().setAttachment(RootContext.HIDDEN_KEY_XID, xid);
            RpcInternalContext.getContext().setAttachment(RootContext.HIDDEN_KEY_BRANCH_TYPE, branchType.name());
            if (txg != null) {
                RpcInternalContext.getContext().setAttachment(RootContext.KEY_TXG, txg);
            }
        } else {
            if (null != rpcXid) {
                RootContext.bind(rpcXid);
                if (StringUtils.equals(BranchType.TCC.name(), rpcBranchType)) {
                    RootContext.bindBranchType(BranchType.TCC);
                }
                if (rpcTxg != null) {
                    RootContext.bindTXG(rpcTxg);
                }
                bind = true;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("bind[{}] to RootContext", rpcXid);
                }
            }
        }
        try {
            return filterInvoker.invoke(sofaRequest);
        } finally {
            if (xid != null) {
                RpcInternalContext.getContext().removeAttachment(RootContext.HIDDEN_KEY_XID);
                RpcInternalContext.getContext().removeAttachment(RootContext.HIDDEN_KEY_BRANCH_TYPE);
                RpcInternalContext.getContext().removeAttachment(RootContext.KEY_TXG);
            }
            if (bind) {
                String unbindXid = RootContext.unbind();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("unbind[{}] from RootContext", unbindXid);
                }
                BranchType previousBranchType = RootContext.getBranchType();
                if (BranchType.TCC == previousBranchType) {
                    RootContext.unbindBranchType();
                }
                RootContext.unbindTXG();
                if (!rpcXid.equalsIgnoreCase(unbindXid)) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("xid in change during RPC from [{}] to [{}]", rpcXid, unbindXid);
                    }
                    if (unbindXid != null) {
                        RootContext.bind(unbindXid);
                        if (LOGGER.isWarnEnabled()) {
                            LOGGER.warn("bind [{}] back to RootContext", unbindXid);
                        }
                        if (BranchType.TCC == previousBranchType) {
                            RootContext.bindBranchType(BranchType.TCC);
                            LOGGER.warn("bind branchType [{}] back to RootContext", previousBranchType);
                        }
                        if (rpcTxg != null) {
                            RootContext.bindTXG(rpcTxg);
                            LOGGER.warn("bind txg [{}] back to RootContext", rpcTxg);
                        }
                    }
                }
            }
        }
    }

    /**
     * get rpc xid
     * @return
     */
    private String getRpcXid(SofaRequest sofaRequest) {
        String rpcXid = (String) sofaRequest.getRequestProp(RootContext.KEY_XID);
        if (rpcXid == null) {
            rpcXid = (String) sofaRequest.getRequestProp(RootContext.KEY_XID.toLowerCase());
        }
        return rpcXid;
    }

    private String getBranchType(SofaRequest sofaRequest) {
        return (String) sofaRequest.getRequestProp(RootContext.KEY_BRANCH_TYPE);
    }

    /**
     * get rpc txg
     * @param sofaRequest
     * @return
     */
    private String getTxg(SofaRequest sofaRequest) {
        return (String) sofaRequest.getRequestProp(RootContext.KEY_TXG);
    }
}
