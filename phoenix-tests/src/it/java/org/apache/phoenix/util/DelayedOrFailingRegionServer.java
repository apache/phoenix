/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.Action;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This is an extended DelayedRegionServer which also allows injecting failure for specific
 * server-side operations for testing purposes.
 */
public class DelayedOrFailingRegionServer extends DelayedRegionServer {

    private static String injectFailureForRegionOfTable = null;
    private static final Logger LOGGER =
            LoggerFactory.getLogger(DelayedOrFailingRegionServer.class);

    public static final String INJECTED_EXCEPTION_STRING = "Injected exception message";

    public DelayedOrFailingRegionServer(Configuration conf)
            throws IOException, InterruptedException {
        super(conf);
    }

    @Override
    protected RSRpcServices createRpcServices() throws IOException {
        return new DelayedOrFailingRSRpcServices(this);
    }

    public static void injectFailureForRegionOfTable(String tableName) {
        injectFailureForRegionOfTable = tableName;
    }

    /**
     * This class injects failure for RPC calls or executes super methods if failure injection is
     * disabled
     */
    public static class DelayedOrFailingRSRpcServices extends DelayedRSRpcServices {

        DelayedOrFailingRSRpcServices(HRegionServer rs) throws IOException {
            super(rs);
        }

        @Override
        public GetResponse get(final RpcController controller,
                final GetRequest request)
                throws ServiceException {
            optionallyInjectFailureIfRegionBelongsToTable(request.getRegion(), "get");
            return super.get(controller, request);
        }


        @Override
        public MutateResponse mutate(final RpcController controller,
                final MutateRequest request) throws  org.apache.hbase.thirdparty.com.google.protobuf.ServiceException {
            optionallyInjectFailureIfRegionBelongsToTable(request.getRegion(), "mutate");
            return super.mutate(controller, request);
        }

        @Override
        public MultiResponse multi(final RpcController controller,
                final MultiRequest request) throws ServiceException {
            for (RegionAction req : request.getRegionActionList()) {
                try {
                    if (doesRegionBelongToTable(req.getRegion())) {
                        boolean isBatchOfOnlyPuts = true;
                        boolean isBatchOfOnlyDeletes = true;
                        for (Action action : req.getActionList()) {
                            MutationType
                                    type =
                                    action.getMutation().getMutateType();
                            if (type.equals(ClientProtos.MutationProto.MutationType.PUT)) {
                                isBatchOfOnlyDeletes = false;
                            } else if (type.equals(ClientProtos.MutationProto.MutationType.DELETE)) {
                                isBatchOfOnlyPuts = false;
                            } else {
                                isBatchOfOnlyPuts = false;
                                isBatchOfOnlyDeletes = false;
                            }
                        }
                        if (isBatchOfOnlyPuts) {
                            optionallyInjectFailureIfRegionBelongsToTable(req.getRegion(), "put");
                        } else if (isBatchOfOnlyDeletes) {
                            optionallyInjectFailureIfRegionBelongsToTable(req.getRegion(), "delete");
                        }
                    }
                } catch (IOException e) {
                    LOGGER.error("Mutate failed with exception: ", e);
                }
            }
            return super.multi(controller, request);
        }

        @Override
        public ScanResponse scan(
                final RpcController controller,
                ClientProtos.ScanRequest request) throws
                ServiceException {
            optionallyInjectFailureIfRegionBelongsToTable(request.getRegion(), "scan");
            return super.scan(controller, request);
        }

        /**
         * If {@link DelayedOrFailingRegionServer#injectFailureForRegionOfTable} matches the name
         * of the table hosted by this RS, then we throw an exception which causes immediate
         * server-side failure
         * @param specifier region specifier
         * @param op scan/get/mutate operation
         * @throws ServiceException exception to be thrown
         */
        private void optionallyInjectFailureIfRegionBelongsToTable(
               RegionSpecifier specifier, String op) throws ServiceException {
            try {
                if (doesRegionBelongToTable(specifier)) {
                    // We have to throw a DoNotRetryIOException, otherwise retries will lead
                    // to timeouts rather than immediate failure
                    throw new ServiceException(new DoNotRetryIOException(INJECTED_EXCEPTION_STRING));
                }
            } catch (IOException e) {
                LOGGER.error("Not injecting failure since getting region during " + op
                        + " failed with: ", e);
            }
        }

        private boolean doesRegionBelongToTable(RegionSpecifier specifier)
                throws IOException {
            return injectFailureForRegionOfTable != null && this.getRegion(specifier).getTableDescriptor().getTableName().getNameAsString()
                    .equals(injectFailureForRegionOfTable);
        }
    }

}
