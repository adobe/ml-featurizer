/** ***********************************************************************
  * ADOBE CONFIDENTIAL
  * ___________________
  * <p>
  * Copyright 2018 Adobe
  * All Rights Reserved.
  * <p>
  * NOTICE: All information contained herein is, and remains
  * the property of Adobe and its suppliers, if any. The intellectual
  * and technical concepts contained herein are proprietary to Adobe
  * and its suppliers and are protected by all applicable intellectual
  * property laws, including trade secret and copyright laws.
  * Dissemination of this information or reproduction of this material
  * is strictly forbidden unless prior written permission is obtained
  * from Adobe.
  * *************************************************************************/
package com.adobe.platform.ml.examples

import com.adobe.platform.ml.feature.binary.string.ConcateColumnsFeaturizer
import org.apache.spark.sql.SparkSession

object ConcatenateFeaturizerExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ConcatenateFeaturizer").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val data = Array((0, "2018-01-02","F","high","red"),
      (1, "2018-02-02","M","high","yellow"),
      (2, "2018-03-02","F","high","red"),
      (3, "2018-04-05","M","low","black"),
      (3, "2018-05-05","F","medium","red"))
    val dataFrame = spark.createDataFrame(data).toDF("id", "date","gender","level","color")

    val featurizer = new ConcateColumnsFeaturizer()
      .setInputCols(Array("gender","level","color"))
      .setOutputCol("g_l_g")

    val featurizedDataFrame = featurizer.transform(dataFrame)
    featurizedDataFrame.show()
  }
}