
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
package org.apache.phoenix.spark;

import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.spark.datasource.v2.PhoenixDataSource;
import org.apache.phoenix.util.QueryBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import scala.Option;
import scala.collection.JavaConverters;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class SparkUtil {

    public static final String APP_NAME = "Java Spark Tests";
    public static final String NUM_EXECUTORS = "local[2]";
    public static final String UI_SHOW_CONSOLE_PROGRESS = "spark.ui.showConsoleProgress";

    public static SparkSession getSparkSession() {
        return SparkSession.builder().appName(APP_NAME).master(NUM_EXECUTORS)
                .config(UI_SHOW_CONSOLE_PROGRESS, false).getOrCreate();
    }

    public static ResultSet executeQuery(Connection conn, QueryBuilder queryBuilder, String url, Configuration config)
            throws SQLException {
        SQLContext sqlContext = getSparkSession().sqlContext();

        boolean forceRowKeyOrder =
                conn.unwrap(PhoenixConnection.class).getQueryServices().getProps()
                        .getBoolean(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, false);
        // if we are forcing row key order we have to add an ORDER BY
        // here we assume that the required columns are in the primary key column order
        String prevOrderBy = queryBuilder.getOrderByClause();
        if (forceRowKeyOrder &&  (queryBuilder.getOrderByClause()==null || queryBuilder.getOrderByClause().isEmpty())) {
            queryBuilder.setOrderByClause(Joiner.on(", ").join(queryBuilder.getRequiredColumns()));
        }

        // create PhoenixRDD using the table name and columns that are required by the query
        // since we don't set the predicate filtering is done after rows are returned from spark
        Dataset phoenixDataSet = getSparkSession().read().format("phoenix")
                .option(DataSourceOptions.TABLE_KEY, queryBuilder.getFullTableName())
                .option(PhoenixDataSource.ZOOKEEPER_URL, url).load();

        phoenixDataSet.createOrReplaceTempView(queryBuilder.getFullTableName());
        Dataset<Row> dataset = sqlContext.sql(queryBuilder.build());
        SparkPlan plan = dataset.queryExecution().executedPlan();
        List<Row> rows = dataset.collectAsList();
        queryBuilder.setOrderByClause(prevOrderBy);
        ResultSet rs = new SparkResultSet(rows, dataset.columns());
        return rs;
    }
}
