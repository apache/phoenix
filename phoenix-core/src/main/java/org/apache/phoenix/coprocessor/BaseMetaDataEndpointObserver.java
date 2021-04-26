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

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.phoenix.coprocessor.PhoenixMetaDataCoprocessorHost.PhoenixMetaDataControllerEnvironment;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;

public class BaseMetaDataEndpointObserver implements MetaDataEndpointObserver, PhoenixCoprocessor{

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {

    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {

    }

    @Override
    public void preGetTable(
            org.apache.hadoop.hbase.coprocessor.ObserverContext<PhoenixMetaDataControllerEnvironment> ctx,
            String tenantId, String tableName, TableName physicalTableName) throws IOException {

    }

    
    @Override
    public void preCreateTable(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId,
            String tableName, TableName physicalTableName, TableName parentPhysicalTableName, PTableType tableType,
            Set<byte[]> familySet, Set<TableName> indexes) throws IOException {

    }

    @Override
    public void preDropTable(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId,
            String tableName, TableName physicalTableName, TableName parentPhysicalTableName, PTableType tableType,
            List<PTable> indexes) throws IOException {

    }

    @Override
    public void preAlterTable(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId,
            String tableName, TableName physicalTableName, TableName parentPhysicalTableName, PTableType type) throws IOException {

    }

    @Override
    public void preGetSchema(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String schemaName)
            throws IOException {

    }

    @Override
    public void preCreateSchema(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String schemaName)
            throws IOException {

    }

    @Override
    public void preDropSchema(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String schemaName) throws IOException {

    }

    @Override
    public void preCreateFunction(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId,
            String functionName) throws IOException {

    }

    @Override
    public void preDropFunction(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId, String functionName)
            throws IOException {}

    @Override
    public void preGetFunctions(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId, String functionName)
            throws IOException {

    }

    @Override
    public void preIndexUpdate(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId,
            String indexName, TableName physicalTableName, TableName parentPhysicalTableName, PIndexState newState)
            throws IOException {

    }

    @Override
    public void preCreateViewAddChildLink(
            final ObserverContext<PhoenixMetaDataControllerEnvironment> ctx,
            final String tableName) throws IOException {}

    @Override
    public void preUpsertTaskDetails(
            final ObserverContext<PhoenixMetaDataControllerEnvironment> ctx,
            final String tableName) throws IOException {
    }
}
