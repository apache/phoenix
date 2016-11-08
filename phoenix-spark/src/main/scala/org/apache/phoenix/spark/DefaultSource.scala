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

import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class DefaultSource extends RelationProvider with CreatableRelationProvider {

  // Override 'RelationProvider.createRelation', this enables DataFrame.load()
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    verifyParameters(parameters)

    new PhoenixRelation(
      parameters("table"),
      parameters("zkUrl"),
      parameters.contains("dateAsTimestamp")
    )(sqlContext)
  }

  // Override 'CreatableRelationProvider.createRelation', this enables DataFrame.save()
  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
                              parameters: Map[String, String], data: DataFrame): BaseRelation = {

    if (!mode.equals(SaveMode.Overwrite)) {
      throw new Exception("SaveMode other than SaveMode.OverWrite is not supported")
    }

    verifyParameters(parameters)

    // Save the DataFrame to Phoenix
    data.saveToPhoenix(parameters("table"), zkUrl = parameters.get("zkUrl"), tenantId = parameters.get("TenantId"))

    // Return a relation of the saved data
    createRelation(sqlContext, parameters)
  }

  // Ensure the required parameters are present
  def verifyParameters(parameters: Map[String, String]): Unit = {
    if (parameters.get("table").isEmpty) throw new RuntimeException("No Phoenix 'table' option defined")
    if (parameters.get("zkUrl").isEmpty) throw new RuntimeException("No Phoenix 'zkUrl' option defined")
  }

}
