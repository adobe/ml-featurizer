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
import org.apache.spark.ml.param.IntParam
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, functions}

private[feature] trait PowerTransformFeaturizerParams extends Params with HasInputCol with HasOutputCol {
  val powerType: IntParam = new IntParam(this, "powerType", "Power function type")

  def getPowerType: Int = $(powerType)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), IntegerType, false)
    StructType(outputFields)
  }
}

class PowerTransformFeaturizer(override val uid: String)
  extends Transformer with PowerTransformFeaturizerParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("powerTransformFeaturizer"))
  setDefault(powerType -> 2)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setPowerType(value: Int): this.type = set(powerType, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val outputSchema = transformSchema(dataset.schema, logging = true)
    val schema = dataset.schema
    val inputType = schema($(inputCol)).dataType
    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(col("*"), (functions.pow($(inputCol), getPowerType.toInt).as($(outputCol), metadata)))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): PowerTransformFeaturizer = defaultCopy(extra)
}

object PowerTransformFeaturizer extends DefaultParamsReadable[PowerTransformFeaturizer] {
  override def load(path: String): PowerTransformFeaturizer = super.load(path)
}
