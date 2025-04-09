/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.replication.log;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Extends DataOutput with a sync method for durability guarantees.
 * Allows abstracting over different underlying output stream implementations
 * like FSDataOutputStream or AsyncFSOutput.
 */
public interface SyncableDataOutput extends DataOutput, Closeable {

    /**
     * Flushes buffered data and ensures durability based on the underlying
     * implementation's guarantees (e.g., hsync for HDFS).
     * @throws IOException if an I/O error occurs during sync.
     */
    void sync() throws IOException;

    /**
     * Returns the current position of the output, including any buffered data.
     * Note: The exact meaning might differ slightly based on the implementation
     * (e.g., synchronous vs asynchronous buffering).
     * @return the current position of the output.
     * @throws IOException if the position cannot be determined.
     */
    long getPos() throws IOException;

}
