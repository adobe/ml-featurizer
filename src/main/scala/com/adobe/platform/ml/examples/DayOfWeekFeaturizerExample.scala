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

import java.sql.Timestamp

import com.adobe.platform.ml.feature.unary.temporal.DayOfWeekFeaturizer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructType, StructField, TimestampType, IntegerType}

object DayOfWeekFeaturizerExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DayOfWeekFeaturizer").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //Sunday: 7, Monday:1, Tuesday:2, Wednesday:3, Thursday:4, Friday:5, Saturday:6
    val data = Array((0, "2018-01-02"),
      (1, "2018-02-02"),
      (2, "2018-03-02"),
      (3, "2018-04-05"),
      (3, "2018-05-05"))
    val dataFrame = spark.createDataFrame(data).toDF("id", "date")

    val featurizer = new DayOfWeekFeaturizer()
      .setInputCol("date")
      .setOutputCol("dayOfWeek")
      .setFormat("yyyy-MM-dd")

    val featurizedDataFrame = featurizer.transform(dataFrame)
    featurizedDataFrame.show()

    val dataWithTime = Array((0, "2018-01-02 050000"),
      (1, "2018-02-02 120000"),
      (2, "2018-03-02 150000"),
      (3, "2018-04-05 200000"),
      (3, "2018-05-05 230000"))

    val dataFrameWithTime = spark.createDataFrame(dataWithTime).toDF("id", "date")
    dataFrameWithTime.printSchema()

    val featurizerHandleTime = new DayOfWeekFeaturizer()
      .setInputCol("date")
      .setOutputCol("dayOfWeek")
      .setFormat("yyyy-MM-dd HHmmss")

    val featurizedDataFrameWithTime = featurizerHandleTime.transform(dataFrameWithTime)
    featurizedDataFrameWithTime.show()

    val dataWithTimestamp = Array(Row(0, Timestamp.valueOf("2018-01-02 05:00:00")),
      Row(1, Timestamp.valueOf("2018-02-02 12:00:00")),
      Row(2, Timestamp.valueOf("2018-03-02 15:00:00")),
      Row(3, Timestamp.valueOf("2018-04-05 20:00:00")),
      Row(3, Timestamp.valueOf("2018-05-05 23:00:00")))

    val schema = List(
      StructField("id", IntegerType, true),
      StructField("date", TimestampType, true)
    )

    val dataFrameWithTimestamp = spark.createDataFrame(
      spark.sparkContext.parallelize(dataWithTimestamp),
      StructType(schema)
    )

    dataFrameWithTimestamp.printSchema()

    val featurizerHandleTimestamp = new DayOfWeekFeaturizer()
      .setInputCol("date")
      .setOutputCol("dayOfWeek")
      .setFormat("yyyy-MM-dd HH:mm:ss")

    val featurizedDataFrameWithTimestamp = featurizerHandleTimestamp.transform(dataFrameWithTimestamp)
    featurizedDataFrameWithTimestamp.show()
  }
}
