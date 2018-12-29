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
import java.time.{ZonedDateTime, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField

import com.adobe.platform.ml.feature.util.{TemporalFeaturizerUtils, HasInputCol, HasOutputCol}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{StructField, StructType}


/**
  * Params for [[MonthOfYearFeaturizer]]
  */
private[feature] trait MonthOfYearFeaturizerParams extends Params with HasInputCol with HasOutputCol {
  final val format: Param[String] = new Param(this, "format", s"Date time format.")
  final val timezone: Param[String] = new Param(this, "timezone", s"Timezone used.")

  /** @group getParam */
  def getFormat: String = $(format)
  def getTimezone: String = $(timezone)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), IntegerType, false)
    StructType(outputFields)
  }
}

class MonthOfYearFeaturizer(override val uid: String)
  extends Transformer with MonthOfYearFeaturizerParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("monthOfYearFeaturizer"))

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setFormat(value: String): this.type = set(format, value)

  def setTimezone(value: String): this.type = set(timezone, value)

  setDefault(format -> "yyyy-MM-dd", timezone -> ZoneId.systemDefault().getId)
  val includedFormats = Set("uuuu-MM-dd HH:mm:ss","uuuu-MM-dd")

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val outputSchema = transformSchema(dataset.schema, logging = true)
    val schema = dataset.schema
    val inputType = schema($(inputCol)).dataType
    val updatedFormats =  TemporalFeaturizerUtils.updateFormats(includedFormats, getFormat)

    val formatter = new DateTimeFormatterBuilder()
      .appendPattern(updatedFormats)
      .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
      .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
      .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
      .toFormatter()
      .withZone(ZoneId.of(getTimezone))

    val toMonthOfYearString = udf {
      in: String => {
        val date = LocalDateTime.parse(in, formatter)
        val  zonedDateTime = ZonedDateTime.of(date, ZoneId.of(getTimezone))
        val month = zonedDateTime.getMonthValue
        month
      }
    }

    val toMonthOfYearTimestamp = udf {
      in: Timestamp => {
        val month = in.toLocalDateTime().getMonthValue
        month
      }
    }

    val metadata = outputSchema($(outputCol)).metadata

    inputType match {
      case StringType => {
        dataset.select(col("*"), toMonthOfYearString(col($(inputCol))).as($(outputCol), metadata))
      }
      case TimestampType => {
        dataset.select(col("*"), toMonthOfYearTimestamp(col($(inputCol))).as($(outputCol), metadata))
      }
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): MonthOfYearFeaturizer = defaultCopy(extra)
}

object MonthOfYearFeaturizer extends DefaultParamsReadable[MonthOfYearFeaturizer] {
  override def load(path: String): MonthOfYearFeaturizer = super.load(path)
}