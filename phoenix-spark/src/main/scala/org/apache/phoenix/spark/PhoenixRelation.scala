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
import org.apache.spark.sql.types.{UTF8String, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.phoenix.util.StringUtil.escapeStringConstant

case class PhoenixRelation(tableName: String, zkUrl: String)(@transient val sqlContext: SQLContext)
    extends BaseRelation with PrunedFilteredScan {

  /*
    This is the buildScan() implementing Spark's PrunedFilteredScan.
    Spark SQL queries with columns or predicates specified will be pushed down
    to us here, and we can pass that on to Phoenix. According to the docs, this
    is an optimization, and the filtering/pruning will be re-evaluated again,
    but this prevents having to load the whole table into Spark first.
  */
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    new PhoenixRDD(
      sqlContext.sparkContext,
      tableName,
      requiredColumns,
      Some(buildFilter(filters)),
      Some(zkUrl),
      new Configuration()
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
      new Configuration()
    ).toDataFrame(sqlContext).schema
  }

  // Attempt to create Phoenix-accepted WHERE clauses from Spark filters,
  // mostly inspired from Spark SQL JDBCRDD and the couchbase-spark-connector
  private def buildFilter(filters: Array[Filter]): String = {
    if (filters.isEmpty) {
      return ""
    }

    val filter = new StringBuilder("")
    var i = 0

    filters.foreach(f => {
      if (i > 0) {
        filter.append(" AND")
      }

      f match {
        case EqualTo(attr, value) => filter.append(s" $attr = ${compileValue(value)}")
        case GreaterThan(attr, value) => filter.append(s" $attr > ${compileValue(value)}")
        case GreaterThanOrEqual(attr, value) => filter.append(s" $attr >= ${compileValue(value)}")
        case LessThan(attr, value) => filter.append(s" $attr < ${compileValue(value)}")
        case LessThanOrEqual(attr, value) => filter.append(s" $attr <= ${compileValue(value)}")
        case IsNull(attr) => filter.append(s" $attr IS NULL")
        case IsNotNull(attr) => filter.append(s" $attr IS NOT NULL")
        case _ => throw new Exception("Unsupported filter")
      }

      i = i + 1
    })

    filter.toString()
  }

  // Helper function to escape string values in SQL queries
  private def compileValue(value: Any): Any = value match {
    case stringValue: String => s"'${escapeStringConstant(stringValue)}'"
    case stringValue: UTF8String => s"'${escapeStringConstant(stringValue.toString)}'"
    case _ => value
  }
}
