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
package com.adobe.platform.ml.feature.unary.temporal

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

import com.adobe.platform.ml.feature.util.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

private[feature] trait PartsOfDayFeaturizerParams extends Params with HasInputCol with HasOutputCol {

  final val format: Param[String] = new Param(this, "format", s"Date time format.")

  /** @group getParam */
  def getFormat: String = $(format)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), IntegerType, false)
    StructType(outputFields)
  }
}

class PartsOfDayFeaturizer(override val uid: String)
  extends Transformer with PartsOfDayFeaturizerParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("partsOfDayFeaturizer"))

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setFormat(value: String): this.type = set(format, value)

  setDefault(format -> "yyyy-MM-dd HH:mm:ss")
  val formatter = new SimpleDateFormat(getFormat)

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val outputSchema = transformSchema(dataset.schema, logging = true)
    val schema = dataset.schema
    val inputType = schema($(inputCol)).dataType

    def toPartOfDay(in: Int) : Int = {
      val output = in match {
        case zero if (in >= 5 && in < 8) => 0
        case one if ( in >= 8 && in < 11) => 1
        case two if  (in >= 11 && in < 14) => 2
        case three if (in >= 14 && in < 19) => 3
        case four if (in >= 19 && in < 22) => 4
        case _ => 5
      }
      output
    }

    val toPartOfDayString = udf {
      in: String => {
        val date = formatter.parse(in)
        val calendar = Calendar.getInstance
        calendar.setTime(date)
        val hour = calendar.get(Calendar.HOUR_OF_DAY)
        val output = toPartOfDay(hour)
        output
      }
    }

    val toPartOfDayTimestamp = udf {
      in: Timestamp => {
        val calendar = Calendar.getInstance
        calendar.setTime(in)
        val hour = calendar.get(Calendar.HOUR_OF_DAY)
        val output = toPartOfDay(hour)
        output
      }
    }

    val metadata = outputSchema($(outputCol)).metadata

    inputType match {
      case StringType => {
        dataset.select(col("*"), toPartOfDayString(col($(inputCol))).as($(outputCol), metadata))
      }
      case TimestampType => {
        dataset.select(col("*"), toPartOfDayTimestamp(col($(inputCol))).as($(outputCol), metadata))
      }
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): PartsOfDayFeaturizer = defaultCopy(extra)
}

object PartsOfDayFeaturizer extends DefaultParamsReadable[PartsOfDayFeaturizer] {
  override def load(path: String): PartsOfDayFeaturizer = super.load(path)
}
