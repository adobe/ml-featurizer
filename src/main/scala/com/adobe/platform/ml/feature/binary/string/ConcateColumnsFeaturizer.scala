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
package com.adobe.platform.ml.feature.binary.string

import com.adobe.platform.ml.feature.util.{HasInputCols, HasOutputCol}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

private[feature] trait ConcateColumnsFeaturizerParams extends Params with HasInputCols with HasOutputCol {
  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    require(get(inputCols).isDefined, "Input cols must be defined first.")
    require(get(outputCol).isDefined, "Output col must be defined first.")
    val inputColsLength = $(inputCols).length
    require(inputColsLength > 0 && inputColsLength < 3, "Input cols must have non-zero length.")
    require($(inputCols).distinct.length == $(inputCols).length, "Input cols must be distinct.")

    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), DoubleType, false)
    StructType(outputFields)
  }
}

class ConcateColumnsFeaturizer(override val uid: String)
  extends Transformer with ConcateColumnsFeaturizerParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("concateColumnsFeaturizer"))

  def setInputCols(values: String*): this.type = setInputCols(values.toArray)

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)


  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val outputSchema = transformSchema(dataset.schema, logging = true)
    val schema = dataset.schema
    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(col("*"), (concat(col(getInputCols(0)), lit(","), col(getInputCols(1)))).as($(outputCol), metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): ConcateColumnsFeaturizer = defaultCopy(extra)
}

object ConcateColumnsFeaturizer extends DefaultParamsReadable[ConcateColumnsFeaturizer] {
  override def load(path: String): ConcateColumnsFeaturizer = super.load(path)
}