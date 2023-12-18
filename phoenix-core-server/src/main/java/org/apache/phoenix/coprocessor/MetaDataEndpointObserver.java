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
package org.apache.phoenix.coprocessor;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.phoenix.coprocessor.PhoenixMetaDataCoprocessorHost.PhoenixMetaDataControllerEnvironment;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;

public interface MetaDataEndpointObserver extends Coprocessor {

    void preGetTable( ObserverContext<PhoenixMetaDataControllerEnvironment> ctx,  String tenantId,String tableName,
             TableName physicalTableName) throws IOException;

    void preCreateTable(final ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, final String tenantId,
            String tableName, TableName physicalTableName, final TableName parentPhysicalTableName,
            PTableType tableType, final Set<byte[]> familySet, Set<TableName> indexes) throws IOException;

    void preDropTable(final ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, final String tenantId,
            final String tableName,TableName physicalTableName, TableName parentPhysicalTableName, PTableType tableType, List<PTable> indexes) throws IOException;

    void preAlterTable(final ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, final String tenantId,final String tableName,
            final TableName physicalTableName,final TableName parentPhysicalTableName, PTableType type) throws IOException;

    void preGetSchema(final ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, final String schemaName)
            throws IOException;

    void preCreateSchema(final ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, final String schemaName)
            throws IOException;

    void preDropSchema(final ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, final String schemaName)
            throws IOException;

    void preCreateFunction(final ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, final String tenantId,
            final String functionName) throws IOException;

    void preDropFunction(final ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, final String tenantId,
            final String functionName) throws IOException;

    void preGetFunctions(final ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, final String tenantId,
            final String functionName) throws IOException;

    void preIndexUpdate(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId,
            String indexName, TableName physicalTableName, TableName parentPhysicalTableName, PIndexState newState) throws IOException;

    void preCreateViewAddChildLink(final ObserverContext<PhoenixMetaDataControllerEnvironment> ctx,
            final String tableName) throws IOException;

    void preUpsertTaskDetails(
          ObserverContext<PhoenixMetaDataControllerEnvironment> ctx,
          String tableName) throws IOException;
}
