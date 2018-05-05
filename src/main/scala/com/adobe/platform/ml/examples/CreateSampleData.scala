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
  **************************************************************************/
package com.adobe.platform.ml.examples

import org.apache.spark.sql.SparkSession

object CreateSampleData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CreateSampleData").master("local").getOrCreate()

    val data = Array(("0", "2018-01-02", 1.0, 2.0),
      ("1", "2018-02-02 12:00:00", 2.5, 3.5),
      ("2", "2018-03-02 06:00:00", 55.0, 1.0),
      ("3", "2018-04-05 21:15:00", 85.0, 9.0),
      ("4", "2018-05-05 08:00:00", 15.0, 5.0),
      ("5", "2016-05-05 00:00:00", 5.0, 25.0),
      ("6", "2016-01-05 03:00:00", 5.0, 5.0),
      ("7", "2016-02-05 09:00:00", 150.0, 5.0),
      ("8", "2017-03-05 10:00:00", 15.0, 15.0),
      ("9", "2017-04-05 08:00:00", 150.0, 35.0),
      ("10", "2016-05-05 05:00:00", 50.0, 25.0))
    val dataFrame = spark.createDataFrame(data).toDF("id", "date", "price1", "price2")
    dataFrame.printSchema()
    dataFrame.coalesce(1).write.mode("overwrite").parquet(args(0))
  }
}
