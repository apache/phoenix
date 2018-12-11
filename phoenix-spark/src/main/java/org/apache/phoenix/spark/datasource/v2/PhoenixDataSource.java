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
package org.apache.phoenix.spark.datasource.v2;

import java.util.Optional;

import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.spark.datasource.v2.reader.PhoenixDataSourceReader;
import org.apache.phoenix.spark.datasource.v2.writer.PhoenixDataSourceWriteOptions;
import org.apache.phoenix.spark.datasource.v2.writer.PhoenixDatasourceWriter;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;

/**
 * Implements the DataSourceV2 api to read and write from Phoenix tables
 */
public class PhoenixDataSource  implements DataSourceV2,  ReadSupport, WriteSupport, DataSourceRegister {

    public static final String SKIP_NORMALIZING_IDENTIFIER = "skipNormalizingIdentifier";
    public static final String ZOOKEEPER_URL = "zkUrl";

    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        return new PhoenixDataSourceReader(options);
    }

    @Override
    public Optional<DataSourceWriter> createWriter(String writeUUID, StructType schema, SaveMode mode,
            DataSourceOptions options) {
        if (!mode.equals(SaveMode.Overwrite)) {
            throw new RuntimeException("SaveMode other than SaveMode.OverWrite is not supported");
        }
        if (!options.tableName().isPresent()) {
            throw new RuntimeException("No Phoenix option " + DataSourceOptions.TABLE_KEY + " defined");
        }
        if (!options.get(PhoenixDataSource.ZOOKEEPER_URL).isPresent()) {
            throw new RuntimeException("No Phoenix option " + PhoenixDataSource.ZOOKEEPER_URL + " defined");
        }

        PhoenixDataSourceWriteOptions writeOptions = createPhoenixDataSourceWriteOptions(options, schema);
        return Optional.of(new PhoenixDatasourceWriter(writeOptions));
    }

    private PhoenixDataSourceWriteOptions createPhoenixDataSourceWriteOptions(DataSourceOptions options,
            StructType schema) {
        String scn = options.get(PhoenixConfigurationUtil.CURRENT_SCN_VALUE).orElse(null);
        String tenantId = options.get(PhoenixRuntime.TENANT_ID_ATTRIB).orElse(null);
        String zkUrl = options.get(ZOOKEEPER_URL).get();
        boolean skipNormalizingIdentifier = options.getBoolean(SKIP_NORMALIZING_IDENTIFIER, false);
        return new PhoenixDataSourceWriteOptions.Builder().setTableName(options.tableName().get())
                .setZkUrl(zkUrl).setScn(scn).setTenantId(tenantId).setSchema(schema)
                .setSkipNormalizingIdentifier(skipNormalizingIdentifier).build();
    }

    @Override
    public String shortName() {
        return "phoenix";
    }
}
