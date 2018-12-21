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
package com.adobe.platform.ml.feature.unary.numeric

import com.adobe.platform.ml.feature.util.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, functions}

private[feature] trait LogTransformFeaturizerParams extends Params with HasInputCol with HasOutputCol {

  final val logType: Param[String] = new Param(this, "logType", s"Log function type.")

  def getLogType: String = $(logType)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), IntegerType, false)
    StructType(outputFields)
  }
}

class LogTransformFeaturizer(override val uid: String)
  extends Transformer with LogTransformFeaturizerParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("logTransformFeaturizer"))

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setLogType(value: String): this.type = set(logType, value)

  setDefault(logType -> "natural")

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val outputSchema = transformSchema(dataset.schema, logging = true)
    val schema = dataset.schema
    val inputType = schema($(inputCol)).dataType
    val metadata = outputSchema($(outputCol)).metadata

    val addOneIfZero = udf { in: Double => if (in == 0) 1.0 else in }

    getLogType match {
      case "natural" => {
        dataset.select(col("*"), functions.log(addOneIfZero(col($(inputCol)))).as($(outputCol), metadata))
      }
      case "log10" => {
        dataset.select(col("*"), functions.log10(addOneIfZero(col($(inputCol)))).as($(outputCol), metadata))
      }
      case "log2" => {
        dataset.select(col("*"), functions.log2(addOneIfZero(col($(inputCol)))).as($(outputCol), metadata))
      }
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): LogTransformFeaturizer = defaultCopy(extra)
}

object LogTransformFeaturizer extends DefaultParamsReadable[LogTransformFeaturizer] {
  override def load(path: String): LogTransformFeaturizer = super.load(path)
}