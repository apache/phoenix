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
package org.apache.phoenix.flume.serializer;

import java.sql.SQLException;
import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurableComponent;

import org.apache.phoenix.util.SQLCloseable;

public interface EventSerializer extends Configurable,ConfigurableComponent,SQLCloseable {

    /**
     * called during the start of the process to initialize the table columns.
     */
    public void initialize() throws SQLException;
    
    /**
     * @param events to be written to HBase.
     * @throws SQLException 
     */
    public void upsertEvents(List<Event> events) throws SQLException;
    
}
