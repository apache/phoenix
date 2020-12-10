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
package org.apache.phoenix.mapreduce.index.automation;

import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;

public class PhoenixMRJobCallable implements Callable<Boolean> {

    private PhoenixAsyncIndex indexInfo;
    private String basePath;
    private Configuration conf;

    public PhoenixMRJobCallable(Configuration conf, final PhoenixAsyncIndex indexInfo,
            String basePath) {
        this.conf = conf;
        this.indexInfo = indexInfo;
        this.basePath = basePath;
    }

    @Override
    public Boolean call() throws Exception {
        StringBuilder commandLineArgBuilder = new StringBuilder();
        commandLineArgBuilder.append(" -dt " + indexInfo.getDataTableName());
        commandLineArgBuilder.append(" -it " + indexInfo.getTableName());
        commandLineArgBuilder.append(" -direct");
        commandLineArgBuilder.append(" -op " + (basePath.endsWith("/") ? basePath : basePath + "/")
                + indexInfo.getTableName());

        if (indexInfo.getTableSchem() != null && indexInfo.getTableSchem().trim().length() > 0) {
            commandLineArgBuilder.append(" -s " + indexInfo.getTableSchem());
        }
        // Setting the configuration here again (in addition to IndexTool.java) to doubly sure
        // configurations are set
        final String qDataTable =
                SchemaUtil.getTableName(indexInfo.getTableSchem(), indexInfo.getDataTableName());
        final String qIndexTable =
                SchemaUtil.getTableName(indexInfo.getTableSchem(), indexInfo.getTableName());
        String physicalIndexTable = qIndexTable;

        if (IndexType.LOCAL.equals(indexInfo.getIndexType())) {
            physicalIndexTable = MetaDataUtil.getLocalIndexTableName(qDataTable);
        }
        conf.set(TableOutputFormat.OUTPUT_TABLE, physicalIndexTable);

        IndexTool tool = new IndexTool();
        tool.setConf(conf);
        int result = tool.run(commandLineArgBuilder.toString().split(" "));
        return result == 0 ? true : false;
    }

}
