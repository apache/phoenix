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
package org.apache.calcite.sql;

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.phoenix.calcite.PhoenixPrepareImpl;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.query.QueryServices;

public class ListJarsTable extends AbstractTable implements ScannableTable {

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
                .add("JAR_LOCATION", SqlTypeName.VARCHAR)
                .build();
    }

    public static final Method LIST_JARS_TABLE_METHOD =
            Types.lookupMethod(ListJarsTable.class, "generate");

    public static ScannableTable generate() {
        return new ListJarsTable();
    }
    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        List<Object[]> filePaths = new ArrayList<Object[]>(1);
        PhoenixConnection phoenixConnection =
                PhoenixPrepareImpl.getPhoenixConnection(root.getRootSchema());
        String dynamicJarsDir =
                phoenixConnection.getQueryServices().getProps()
                        .get(QueryServices.DYNAMIC_JARS_DIR_KEY);
        if (dynamicJarsDir == null) {
            throw new RuntimeException(new SQLException(QueryServices.DYNAMIC_JARS_DIR_KEY
                    + " is not configured for the listing the jars."));
        }
        dynamicJarsDir = dynamicJarsDir.endsWith("/") ? dynamicJarsDir : dynamicJarsDir + '/';
        Configuration conf = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        Path dynamicJarsDirPath = new Path(dynamicJarsDir);
        FileSystem fs;
        try {
            fs = dynamicJarsDirPath.getFileSystem(conf);
            RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(dynamicJarsDirPath, true);
            if (listFiles != null) {
                while (listFiles.hasNext()) {
                    LocatedFileStatus file = listFiles.next();
                    filePaths.add(new Object[] { file.getPath().toString() });
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Linq4j.asEnumerable(filePaths);
    }

}
