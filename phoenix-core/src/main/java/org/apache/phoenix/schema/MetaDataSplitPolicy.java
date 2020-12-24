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
package org.apache.phoenix.schema;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;

public class MetaDataSplitPolicy extends SplitOnLeadingVarCharColumnsPolicy {

    private boolean allowSystemCatalogToSplit() {
        Configuration conf = getConf();
        boolean allowSplittableSystemCatalogRollback =
                conf.getBoolean(QueryServices.ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK,
                    QueryServicesOptions.DEFAULT_ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK);
        boolean allowSystemCatalogToSplit =
                conf.getBoolean(QueryServices.SYSTEM_CATALOG_SPLITTABLE,
                    QueryServicesOptions.DEFAULT_SYSTEM_CATALOG_SPLITTABLE);
        return allowSystemCatalogToSplit && !allowSplittableSystemCatalogRollback;
    }

    //This only exists in HBase 2.4+
    @Override
    protected boolean canSplit() {
        return super.canSplit() && allowSystemCatalogToSplit();
    }

    @Override
    protected boolean shouldSplit() {
        return super.shouldSplit() && allowSystemCatalogToSplit();
    }

    @Override
    protected int getColumnToSplitAt() {
        // SYSTEM.CATALOG rowkey is (tenant id, schema name, table name, column name,
        // column family) ensure all meta data rows for a given schema are in the same
        // region (indexes and tables are in the same schema as we lock the parent table
        // when modifying an index)
        return 2;
    }

}
