/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.adobe.platform.ml.feature.group

import com.adobe.platform.ml.feature.util.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

private[feature] trait GroupByFeaturizerParams extends Params with HasInputCol with HasOutputCol {

  final val aggregateType: Param[String] = new Param(this, "aggregateType", s"Aggregate type: min, max, count, avg,.")
  final val aggregateCol: Param[String] = new Param(this, "aggregateCol", s"Column to be aggregated")

  def getAggregateType: String = $(aggregateType)
  def getAggregateCol:String = $(aggregateCol)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), IntegerType, false)
    StructType(outputFields)
  }
}

class GroupByFeaturizer(override val uid: String)
  extends Transformer with GroupByFeaturizerParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("groupByFeaturizer"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setAggregateType(value: String): this.type = set(aggregateType, value)

  def setAggregateCol(value: String): this.type = set(aggregateCol, value)

  setDefault(aggregateType -> "count")

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val outputSchema = transformSchema(dataset.schema, logging = true)
    val schema = dataset.schema
    val inputType = schema($(inputCol)).dataType
    val metadata = outputSchema($(outputCol)).metadata
    getAggregateType match {
      case "count" => {
        val counts = dataset.groupBy($(inputCol)).count().alias($(outputCol))
        val result = dataset.join(counts, Seq($(inputCol)))
        result
      }
      case "ratio" => {
        val ratios = dataset.groupBy($(inputCol)).count().withColumn($(outputCol), col("count") / sum("count").over()).drop("count")
        val result = dataset.join(ratios, Seq($(inputCol)))
        result
      }
      case "min" => {
        val mins = dataset.groupBy($(inputCol)).min($(aggregateCol)).alias($(outputCol))
        val result = dataset.join(mins, Seq($(inputCol)))
        result
      }
      case "max" => {
        val maxs = dataset.groupBy($(inputCol)).max($(aggregateCol)).alias($(outputCol))
        val result = dataset.join(maxs, Seq($(inputCol)))
        result
      }
      case "avg" => {
        val avgs = dataset.groupBy($(inputCol)).avg($(aggregateCol)).alias($(outputCol))
        val result = dataset.join(avgs, Seq($(inputCol)))
        result
      }
      case "sum" => {
        val sums = dataset.groupBy($(inputCol)).sum($(aggregateCol)).alias($(outputCol))
        val result = dataset.join(sums, Seq($(inputCol)))
        result
      }
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): GroupByFeaturizer = defaultCopy(extra)
}

object GroupByFeaturizer extends DefaultParamsReadable[GroupByFeaturizer] {
  override def load(path: String): GroupByFeaturizer = super.load(path)
}