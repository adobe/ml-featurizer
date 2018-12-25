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

package com.adobe.platform.ml.feature.unary.numeric

import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.sql.Row
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}


class LogTransformFeaturizerSuite extends MLTest with DefaultReadWriteTest {
  import testImplicits._

  def assertResult: Row => Unit = {
    case Row(transformedValue:Double, expectedValue:Double) =>
      assert(transformedValue ~== expectedValue absTol 1E-5,
        "The transformed value is not correct after log transform.")
  }

  test("params") {
    ParamsSuite.checkParams(new LogTransformFeaturizer())
  }

  test("Default Log Transform") {
    val data = Seq(100000, 10, 1, 1000000, 10000000, 1000, 20000)
    val expected = Seq(11.51293, 2.30259, 0.0, 13.81551, 16.11810, 6.90776, 9.90349)
    val df = data.zip(expected).toSeq.toDF("price", "expectedPrice")

    val logFeaturizer = new LogTransformFeaturizer()
      .setInputCol("price")
      .setOutputCol("logPrice")

    testTransformer[(Double, Double)](df, logFeaturizer, "logPrice", "expectedPrice")(
      assertResult)
  }


  test("Log base 10 Transform") {
    val data = Seq(100000, 10, 1, 1000000, 10000000, 1000, 20000)
    val expected = Seq(5.0, 1.0, 0.0, 6.0, 7.0, 3.0, 4.30102)
    val df = data.zip(expected).toSeq.toDF("price", "expectedPrice")

    val logFeaturizer = new LogTransformFeaturizer()
      .setInputCol("price")
      .setOutputCol("logPrice")
    .setLogType("log10")

    testTransformer[(Double, Double)](df, logFeaturizer, "logPrice", "expectedPrice")(
      assertResult)
  }


  test("Natural Log Transform") {
    val data = Seq(100000, 10, 1, 1000000, 10000000, 1000, 20000)
    val expected = Seq(11.51293, 2.30259, 0.0, 13.81551, 16.11810, 6.90776, 9.90349)
    val df = data.zip(expected).toSeq.toDF("price", "expectedPrice")

    val logFeaturizer = new LogTransformFeaturizer()
      .setInputCol("price")
      .setOutputCol("logPrice")
      .setLogType("natural")

    testTransformer[(Double, Double)](df, logFeaturizer, "logPrice", "expectedPrice")(
      assertResult)
  }

  test("Log base 2 Transform") {
    val data = Seq(100000, 10, 1, 1000000, 10000000, 1000, 20000)
    val expected = Seq(16.60964, 3.32193, 0.0, 19.93157, 23.25350, 9.96578, 14.28771)
    val df = data.zip(expected).toSeq.toDF("price", "expectedPrice")

    val logFeaturizer = new LogTransformFeaturizer()
      .setInputCol("price")
      .setOutputCol("logPrice")
      .setLogType("log2")

    testTransformer[(Double, Double)](df, logFeaturizer, "logPrice", "expectedPrice")(
      assertResult)
  }

  test("LogTransformFeaturizer read/write") {
    val t = new LogTransformFeaturizer()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setLogType("log2")
    testDefaultReadWrite(t)
  }
}
