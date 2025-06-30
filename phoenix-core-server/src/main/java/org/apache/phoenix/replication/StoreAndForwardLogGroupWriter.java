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
package org.apache.phoenix.replication;

import java.io.IOException;

import org.apache.phoenix.replication.log.LogFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Store-and-forward replication implementation of ReplicationLogGroupWriter.
 * <p>
 * This class is a stub implementation for future store-and-forward replication functionality.
 * Store-and-forward mode is used when the standby cluster is temporarily unavailable - mutations
 * are stored locally and forwarded when connectivity is restored.
 * <p>
 * Currently this is a stub that throws UnsupportedOperationException for the abstract methods.
 * Future implementation will include:
 * <ul>
 *   <li>Local storage of mutations when standby is unavailable</li>
 *   <li>Background forwarding when connectivity is restored</li>
 *   <li>Proper error handling and retry logic</li>
 *   <li>Integration with HA state management</li>
 *   <li>Dual-mode operation: local storage + forwarding</li>
 * </ul>
 */
public class StoreAndForwardLogGroupWriter extends ReplicationLogGroupWriter {

    private static final Logger LOG = LoggerFactory.getLogger(StoreAndForwardLogGroupWriter.class);

    /**
     * Constructor for StoreAndForwardLogGroupWriter.
     */
    public StoreAndForwardLogGroupWriter(ReplicationLogGroup logGroup) {
        super(logGroup);
        LOG.debug("Created StoreAndForwardLogGroupWriter for HA Group: {}",
            logGroup.getHaGroupName());
    }

    @Override
    public void init() throws IOException {
        // TODO
    }

    @Override
    public void close() {
        // TODO
    }

    @Override
    protected void initializeFileSystems() throws IOException {
        // TODO
    }

    @Override
    protected void initializeReplicationShardDirectoryManager() {
        // TODO
    }

    @Override
    protected LogFileWriter createNewWriter() throws IOException {
        // TODO
        return null;
    }
}
