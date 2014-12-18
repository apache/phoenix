/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.pig.util;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.SchemaType;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;

import com.google.common.base.Preconditions;

/**
 * 
 * Utility to generate the ResourceSchema from the list of {@link ColumnInfo}
 *
 */
public final class PhoenixPigSchemaUtil {

    private static final Log LOG = LogFactory.getLog(PhoenixPigSchemaUtil.class);
    
    private PhoenixPigSchemaUtil() {
    }
    
public static ResourceSchema getResourceSchema(final Configuration configuration) throws IOException {
        
        final ResourceSchema schema = new ResourceSchema();
        try {
            List<ColumnInfo> columns = null;
            final SchemaType schemaType = PhoenixConfigurationUtil.getSchemaType(configuration);
            if(SchemaType.QUERY.equals(schemaType)) {
                final String sqlQuery = PhoenixConfigurationUtil.getSelectStatement(configuration);
                Preconditions.checkNotNull(sqlQuery, "No Sql Query exists within the configuration");
                final SqlQueryToColumnInfoFunction function = new SqlQueryToColumnInfoFunction(configuration);
                columns = function.apply(sqlQuery);
            } else {
                columns = PhoenixConfigurationUtil.getSelectColumnMetadataList(configuration);
            }
            ResourceFieldSchema fields[] = new ResourceFieldSchema[columns.size()];
            int i = 0;
            for(ColumnInfo cinfo : columns) {
                int sqlType = cinfo.getSqlType();
                PDataType phoenixDataType = PDataType.fromTypeId(sqlType);
                byte pigType = TypeUtil.getPigDataTypeForPhoenixType(phoenixDataType);
                ResourceFieldSchema field = new ResourceFieldSchema();
                field.setType(pigType).setName(cinfo.getDisplayName());
                fields[i++] = field;
            }
            schema.setFields(fields);    
        } catch(SQLException sqle) {
            LOG.error(String.format("Error: SQLException [%s] ",sqle.getMessage()));
            throw new IOException(sqle);
        }
        
        return schema;
    }
}
