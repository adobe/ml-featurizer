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

import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.sql.Row


class MonthOfYearFeaturizerSuite extends MLTest with DefaultReadWriteTest {
  import testImplicits._

  def assertResult: Row => Unit = {
    case Row(transformedValue:Int, expectedValue:Int) =>
      assert(transformedValue === expectedValue,
        "The transformed value is not correct after month of year transform.")
  }

  test("params") {
    ParamsSuite.checkParams(new MonthOfYearFeaturizer())
  }

  test("Default Month of Year Transform") {
    val data = Seq("2018-08-01 00:00:00", "2018-05-25 12:00:00")
    val expected = Seq(8, 5)
    val df = data.zip(expected).toSeq.toDF("created", "expectedMonthOfYear")

    val featurizer = new MonthOfYearFeaturizer()
      .setInputCol("created")
      .setOutputCol("monthOfYear")
      .setFormat("yyyy-MM-dd HH:mm:ss")

    testTransformer[(String, Int)](df, featurizer, "monthOfYear", "expectedMonthOfYear")(
      assertResult)
  }

  test("Month of Year Transform with format yyyy-MM-dd HH:mm") {
    val data = Seq("2018-08-01 01:00", "2018-05-25 12:00", "2019-03-15 00:00", "2015-09-03 18:00")
    val expected = Seq(8, 5, 3, 9)
    val df = data.zip(expected).toSeq.toDF("created", "expectedMonthOfYear")

    val featurizer = new MonthOfYearFeaturizer()
      .setInputCol("created")
      .setOutputCol("monthOfYear")
      .setFormat("yyyy-MM-dd HH:mm")

    testTransformer[(String, Int)](df, featurizer, "monthOfYear", "expectedMonthOfYear")(
      assertResult)
  }


  test("Month of Year Transform with format yyyyMMddHHmmss") {
    val data = Seq("20180801010000", "20180525120000", "20190315000000", "20150903180000")
    val expected = Seq(8, 5, 3, 9)
    val df = data.zip(expected).toSeq.toDF("created", "expectedMonthOfYear")

    val featurizer = new MonthOfYearFeaturizer()
      .setInputCol("created")
      .setOutputCol("monthOfYear")
      .setFormat("yyyyMMddHHmmss")
      .setTimezone("US/Central")

    testTransformer[(String, Int)](df, featurizer, "monthOfYear", "expectedMonthOfYear")(
      assertResult)
  }


  test("MonthOfYearFeaturizer read/write") {
    val t = new MonthOfYearFeaturizer()
      .setInputCol("created")
      .setOutputCol("monthOfYear")
      .setFormat("yyyy-MM-dd HH:mm:ss")
      .setTimezone("US/Central")
    testDefaultReadWrite(t)
  }
}
