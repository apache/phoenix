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
package org.apache.phoenix.spark

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

@deprecated("Use the DataSource V2 API implementation (see PhoenixDataSource)")
case class PhoenixRelation(tableName: String, zkUrl: String, dateAsTimestamp: Boolean = false)(@transient val sqlContext: SQLContext)
    extends BaseRelation with PrunedFilteredScan {

  /*
    This is the buildScan() implementing Spark's PrunedFilteredScan.
    Spark SQL queries with columns or predicates specified will be pushed down
    to us here, and we can pass that on to Phoenix. According to the docs, this
    is an optimization, and the filtering/pruning will be re-evaluated again,
    but this prevents having to load the whole table into Spark first.
  */
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val(pushedFilters, _, _) = new FilterExpressionCompiler().pushFilters(filters)
    new PhoenixRDD(
      sqlContext.sparkContext,
      tableName,
      requiredColumns,
      Some(pushedFilters),
      Some(zkUrl),
      new Configuration(),
      dateAsTimestamp
    ).toDataFrame(sqlContext).rdd
  }

  // Required by BaseRelation, this will return the full schema for a table
  override def schema: StructType = {
    new PhoenixRDD(
      sqlContext.sparkContext,
      tableName,
      Seq(),
      None,
      Some(zkUrl),
      new Configuration(),
      dateAsTimestamp
    ).toDataFrame(sqlContext).schema
  }


  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    val (_, unhandledFilters, _) = new FilterExpressionCompiler().pushFilters(filters)
    unhandledFilters
  }

}
