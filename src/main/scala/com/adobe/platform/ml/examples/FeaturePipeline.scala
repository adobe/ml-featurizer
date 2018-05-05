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
package com.adobe.platform.ml.examples

import com.adobe.platform.ml.feature.binary.numeric.AdditionFeaturizer
import com.adobe.platform.ml.feature.unary.temporal.{DayOfWeekFeaturizer, MonthOfYearFeaturizer, WeekendFeaturizer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.SparkSession

object FeaturePipeline {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("FeaturePipeline").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val data = Array((0, "2018-01-02", 1.0, 2.0, "mercedes"),
      (1, "2018-02-02", 2.5, 3.5, "lexus"),
      (2, "2018-03-02", 5.0, 1.0, "toyota"),
      (3, "2018-04-05", 8.0, 9.0, "tesla"),
      (4, "2018-05-05", 1.0, 5.0, "bmw"),
      (4, "2018-05-05", 1.0, 5.0, "bmw"))
    val dataFrame = spark.createDataFrame(data).toDF("id", "date", "price1", "price2", "brand")

    val dayOfWeekfeaturizer = new DayOfWeekFeaturizer()
      .setInputCol("date")
      .setOutputCol("dayOfWeek")
      .setFormat("yyyy-MM-dd")

    val monthOfYearfeaturizer = new MonthOfYearFeaturizer()
      .setInputCol("date")
      .setOutputCol("monthOfYear")
      .setFormat("yyyy-MM-dd")

    val weekendFeaturizer = new WeekendFeaturizer()
      .setInputCol("date")
      .setOutputCol("isWeekend")
      .setFormat("yyyy-MM-dd")

    val additionFeaturizer = new AdditionFeaturizer()
      .setInputCols("price1", "price2")
      .setOutputCol("price1_add_price2")

    val indexer = new StringIndexer()
      .setInputCol("brand")
      .setOutputCol("brandIndex")

    val encoder = new OneHotEncoder()
      .setInputCol("brandIndex")
      .setOutputCol("brandVector")

    val pipeline = new Pipeline()
      .setStages(Array(dayOfWeekfeaturizer, monthOfYearfeaturizer, weekendFeaturizer, additionFeaturizer,
        indexer, encoder))
    val model = pipeline.fit(dataFrame)
    model.transform(dataFrame).show()
  }
}
