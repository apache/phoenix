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
package org.apache.phoenix.log;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Used by the event handler to write RingBufferEvent, this is done in a separate thread from the application configured
 * during disruptor
 */
public interface LogWriter {
    /**
     * Called by ring buffer event handler to write RingBufferEvent
     * 
     * @param event
     * @throws SQLException
     * @throws IOException
     */
    void write(RingBufferEvent event) throws SQLException, IOException;

    /**
     * will be called when disruptor is getting shutdown
     * 
     * @throws IOException
     * @throws SQLException 
     */

    void close() throws IOException, SQLException;

    /**
     * if writer is closed and cannot write further event
     * 
     * @return
     */
    boolean isClosed();
}
