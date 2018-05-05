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

import com.adobe.platform.ml.feature.unary.temporal.HourOfDayFeaturizer

import org.apache.spark.sql.SparkSession

object HourOfDayFeaturizerExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("HourOfDayFeaturizer").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val data = Array((0, "2018-01-02 00:20:00"),
      (1, "2018-02-02 12:50:00"),
      (2, "2018-03-02 13:30:00"),
      (3, "2018-04-05 14:10:00"),
      (4, "2018-05-05 09:55:00"),
      (5, "2018-01-06 16:10:00"),
      (6, "2018-01-07 18:44:00"))
    val dataFrame = spark.createDataFrame(data).toDF("id", "date")

    val featurizer = new HourOfDayFeaturizer()
      .setInputCol("date")
      .setOutputCol("hourOfDay")
      .setFormat("yyyy-MM-dd HH:mm:ss")

    val featurizedDataFrame = featurizer.transform(dataFrame)
    featurizedDataFrame.show()
  }
}
