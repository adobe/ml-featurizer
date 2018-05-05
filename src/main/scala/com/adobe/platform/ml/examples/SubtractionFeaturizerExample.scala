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

import com.adobe.platform.ml.feature.binary.numeric.SubtractionFeaturizer
import org.apache.spark.sql.SparkSession

object SubtractionFeaturizerExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SubtractionFeaturizer").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val data = Array((0, "2018-01-02", 1.0, 2.0),
      (1, "2018-02-02", 2.5, 3.5),
      (2, "2018-03-02", 5.0, 1.0),
      (3, "2018-04-05", 8.0, 9.0),
      (3, "2018-05-05", 1.0, 5.0))
    val dataFrame = spark.createDataFrame(data).toDF("id", "date", "price1", "price2")

    val featurizer = new SubtractionFeaturizer()
      .setInputCols("price1", "price2")
      .setOutputCol("price1_price2")

    val featurizedDataFrame = featurizer.transform(dataFrame)
    featurizedDataFrame.show()

  }
}
