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
package org.apache.phoenix.end2end;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.phoenix.coprocessor.PhoenixRegionServerEndpoint;
import org.apache.phoenix.coprocessor.generated.RegionServerEndpointProtos;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.phoenix.query.QueryServices.PHOENIX_METADATA_CACHE_INVALIDATION_TIMEOUT_MS;
import static org.apache.phoenix.query.QueryServices.PHOENIX_METADATA_CACHE_INVALIDATION_TIMEOUT_MS_DEFAULT;

public class FailingPhoenixRegionServerEndpoint extends PhoenixRegionServerEndpoint {
    private static final Logger LOGGER = LoggerFactory.getLogger(FailingPhoenixRegionServerEndpoint.class);

    private boolean throwException;
    private boolean shouldSleep;
    private boolean failFirstAndThenSucceed;
    private int attempt = 0;

    @Override
    public void invalidateServerMetadataCache(RpcController controller,
            RegionServerEndpointProtos.InvalidateServerMetadataCacheRequest request,
            RpcCallback<RegionServerEndpointProtos.InvalidateServerMetadataCacheResponse> done) {
        long metadataCacheInvalidationTimeoutMs = conf.getLong(
                PHOENIX_METADATA_CACHE_INVALIDATION_TIMEOUT_MS,
                PHOENIX_METADATA_CACHE_INVALIDATION_TIMEOUT_MS_DEFAULT);

        if (throwException == true) {
            IOException ioe = new IOException("On purpose");
            ProtobufUtil.setControllerException(controller, ioe);
            return;
        } else if (shouldSleep) {
            try {
                // Sleeping for 2 seconds more than metadataCacheInvalidationTimeoutMs.
                Thread.sleep(metadataCacheInvalidationTimeoutMs + 2000);
            } catch (InterruptedException e) {
                LOGGER.warn("Exception while sleeping in FailingPhoenixRegionServerEndpoint", e);
            }
        } else if (failFirstAndThenSucceed) {
            if (attempt == 0) {
                IOException ioe = new IOException("On purpose");
                ProtobufUtil.setControllerException(controller, ioe);
                attempt++;
            }
        }
        else {
            LOGGER.info("Invalidating server metadata cache");
        }
    }

    public void throwException() {
        reset();
        this.throwException = true;
    }

    public void sleep() {
        reset();
        this.shouldSleep = true;
    }

    public void failFirstAndThenSucceed() {
        reset();
        failFirstAndThenSucceed = true;
    }
    private void reset() {
        this.shouldSleep = false;
        this.throwException = false;
        this.failFirstAndThenSucceed = false;
    }
}
