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
package org.apache.phoenix.flume;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public final class SchemaHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(SchemaHandler.class);
    
    private SchemaHandler() {
		throw new AssertionError("Must not instantiate this class");
	}

    public static boolean createTable(Connection connection, String createTableDdl) {
        Preconditions.checkNotNull(connection);
        Preconditions.checkNotNull(createTableDdl); 
        boolean status  = true;
        try {
            status = connection.createStatement().execute(createTableDdl);
        } catch (SQLException e) {
            logger.error("An error occurred during executing the create table ddl {} ",createTableDdl);
            Throwables.propagate(e);
        }
        return status;
        
    }

}
