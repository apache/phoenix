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

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.coprocessor.generated.ChildLinkMetaDataProtos.CreateViewAddChildLinkRequest;
import org.apache.phoenix.coprocessor.generated.ChildLinkMetaDataProtos.ChildLinkMetaDataService;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataResponse;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.phoenix.coprocessor.MetaDataEndpointImpl.mutateRowsWithLocks;

/**
 * Endpoint co-processor through which Phoenix metadata mutations for SYSTEM.CHILD_LINK flow.
 * The {@code parent->child } links ({@link org.apache.phoenix.schema.PTable.LinkType#CHILD_TABLE})
 * are stored in the SYSTEM.CHILD_LINK table.
 */
public class ChildLinkMetaDataEndpoint extends ChildLinkMetaDataService implements RegionCoprocessor {

	private static final Logger LOGGER = LoggerFactory.getLogger(ChildLinkMetaDataEndpoint.class);
    private RegionCoprocessorEnvironment env;
    private PhoenixMetaDataCoprocessorHost phoenixAccessCoprocessorHost;
    private boolean accessCheckEnabled;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
        this.phoenixAccessCoprocessorHost = new PhoenixMetaDataCoprocessorHost(this.env);
        this.accessCheckEnabled = env.getConfiguration().getBoolean(QueryServices.PHOENIX_ACLS_ENABLED,
            QueryServicesOptions.DEFAULT_PHOENIX_ACLS_ENABLED);
    }

	@Override
	public Iterable<Service> getServices() {
		return Collections.singleton(this);
	}

    @Override
    public void createViewAddChildLink(RpcController controller,
            CreateViewAddChildLinkRequest request, RpcCallback<MetaDataResponse> done) {

        MetaDataResponse.Builder builder = MetaDataResponse.newBuilder();
        try {
            List<Mutation> childLinkMutations = ProtobufUtil.getMutations(request);
            if (childLinkMutations.isEmpty()) {
                done.run(builder.build());
                return;
            }
            byte[][] rowKeyMetaData = new byte[3][];
            MetaDataUtil.getTenantIdAndSchemaAndTableName(childLinkMutations, rowKeyMetaData);
            byte[] parentSchemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
            byte[] parentTableName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
            String fullparentTableName = SchemaUtil.getTableName(parentSchemaName, parentTableName);

            getCoprocessorHost().preCreateViewAddChildLink(fullparentTableName);

            // From 4.15 the parent->child links are stored in a separate table SYSTEM.CHILD_LINK
            mutateRowsWithLocks(this.accessCheckEnabled, this.env.getRegion(), childLinkMutations,
                Collections.<byte[]>emptySet(), HConstants.NO_NONCE, HConstants.NO_NONCE);

        } catch (Throwable t) {
            LOGGER.error("Unable to write mutations to " +
                    PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME, t);
            builder.setReturnCode(MetaDataProtos.MutationCode.UNABLE_TO_CREATE_CHILD_LINK);
            builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
            done.run(builder.build());
        }
	}

	private PhoenixMetaDataCoprocessorHost getCoprocessorHost() {
		return phoenixAccessCoprocessorHost;
	}

}
