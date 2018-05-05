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

import com.adobe.platform.ml.feature.unary.temporal.MonthOfYearFeaturizer
import org.apache.spark.sql.SparkSession

object MonthOfYearFeaturizerExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MonthOfYearFeaturizer").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val data = Array((0, "2018-01-02"),
      (1, "2018-02-02"),
      (2, "2018-03-02"),
      (3, "2018-04-05"),
      (3, "2018-05-05"),
      (4, "2017-12-05"),
      (5, "2017-01-15"))
    val dataFrame = spark.createDataFrame(data).toDF("id", "date")

    val featurizer = new MonthOfYearFeaturizer()
      .setInputCol("date")
      .setOutputCol("monthOfYear")
      .setFormat("yyyy-MM-dd")

    val featurizedDataFrame = featurizer.transform(dataFrame)
    featurizedDataFrame.show()
  }
}
