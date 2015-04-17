package org.apache.phoenix.spark

import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}
import org.apache.spark.sql.sources.{CreatableRelationProvider, BaseRelation, RelationProvider}
import org.apache.phoenix.spark._

class DefaultSource extends RelationProvider with CreatableRelationProvider {

  // Override 'RelationProvider.createRelation', this enables DataFrame.load()
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    verifyParameters(parameters)

    new PhoenixRelation(
      parameters("table"),
      parameters("zkUrl")
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
    data.saveToPhoenix(parameters("table"), zkUrl = parameters.get("zkUrl"))

    // Return a relation of the saved data
    createRelation(sqlContext, parameters)
  }

  // Ensure the required parameters are present
  def verifyParameters(parameters: Map[String, String]): Unit = {
    if (parameters.get("table").isEmpty) throw new RuntimeException("No Phoenix 'table' option defined")
    if (parameters.get("zkUrl").isEmpty) throw new RuntimeException("No Phoenix 'zkUrl' option defined")
  }
}
