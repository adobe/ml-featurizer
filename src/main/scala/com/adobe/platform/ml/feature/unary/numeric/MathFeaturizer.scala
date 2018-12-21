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
import org.apache.spark.sql.{DataFrame, Dataset, functions}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

private[feature] trait MathFeaturizerParams extends Params with HasInputCol with HasOutputCol {

  final val mathFunction: Param[String] = new Param(this, "mathFunction", s"Math function type to apply.")

  def getMathFunction: String = $(mathFunction)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), IntegerType, false)
    StructType(outputFields)
  }
}

class MathFeaturizer(override val uid: String)
  extends Transformer with MathFeaturizerParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("mathFeaturizer"))

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setMathFunction(value: String): this.type = set(mathFunction, value)

  setDefault(mathFunction -> "sqrt")

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val outputSchema = transformSchema(dataset.schema, logging = true)
    val schema = dataset.schema
    val inputType = schema($(inputCol)).dataType
    val metadata = outputSchema($(outputCol)).metadata
    getMathFunction match {
      case "sqrt" => {
        dataset.select(col("*"), (functions.sqrt($(inputCol)).as($(outputCol), metadata)))
      }
      case "log10+1" => {
        dataset.select(col("*"), ((functions.log10($(inputCol))+1).as($(outputCol), metadata)))
      }
      case "log10" => {
        dataset.select(col("*"), (functions.log10($(inputCol)).as($(outputCol), metadata)))
      }
      case "log2+1" => {
        dataset.select(col("*"), ((functions.log2($(inputCol))+1).as($(outputCol), metadata)))
      }
      case "log2" => {
        dataset.select(col("*"), (functions.log2($(inputCol)).as($(outputCol), metadata)))
      }
      case "square" => {
        dataset.select(col("*"), (functions.pow($(inputCol),2).as($(outputCol), metadata)))
      }
      case "exp" => {
        dataset.select(col("*"), (functions.exp($(inputCol)).as($(outputCol), metadata)))
      }
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): MathFeaturizer = defaultCopy(extra)
}

object MathFeaturizer extends DefaultParamsReadable[MathFeaturizer] {
  override def load(path: String): MathFeaturizer = super.load(path)
}