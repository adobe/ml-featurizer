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

import com.adobe.platform.ml.feature.group.GroupByFeaturizer
import org.apache.spark.sql.SparkSession

object GroupByFeaturizerExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("GroupByFeaturizer").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val data = Array((0, "2018-01-02", 15.00),
      (1, "2018-02-02", 5.00),
      (2, "2018-03-02", 10.00),
      (2, "2018-05-05", 20.00),
      (3, "2018-04-05", 100.00),
      (3, "2018-05-05", 500.00),
      (3, "2018-05-08", 300.00))

    val dataFrame = spark.createDataFrame(data).toDF("id", "date", "spent")
    val groupBySumFeaturizer = new GroupByFeaturizer()
      .setInputCol("id")
      .setAggregateCol("spent")
      .setOutputCol("totalSpent")
      .setAggregateType("sum")

    var featurizedDataFrame = groupBySumFeaturizer.transform(dataFrame)
    featurizedDataFrame.show()
  }
}
