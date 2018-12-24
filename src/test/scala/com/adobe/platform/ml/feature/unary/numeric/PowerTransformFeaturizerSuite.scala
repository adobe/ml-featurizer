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


class PowerTransformFeaturizerSuite extends MLTest with DefaultReadWriteTest {
  import testImplicits._

  def assertResult: Row => Unit = {
    case Row(originalValue:Double, transformedValue:Double) =>
      assert(originalValue ~== transformedValue absTol 1E-5,
        "The transformed value is not correct after power transform.")
  }

  test("params") {
    ParamsSuite.checkParams(new PowerTransformFeaturizer())
  }

  test("Default Power Transform") {
    val data = Seq(1, 2, 3, 4, 5, 10, 20000, -1, -2)
    val expected = Seq(1.0, 4.0, 9.0, 16.0, 25.0, 100.0, 400000000.0, 1.0, 4.0)
    val df = data.zip(expected).toSeq.toDF("price", "expectedPrice")

    val featurizer = new PowerTransformFeaturizer()
      .setInputCol("price")
      .setOutputCol("transformedPrice")

    testTransformer[(Double, Double)](df, featurizer, "transformedPrice", "expectedPrice")(
      assertResult)
  }

  test("Power 3 Power Transform") {
    val data = Seq(1, 2, 3, 4, 5, 10, 20000)
    val expected = Seq(1.0, 8.0, 27.0, 64.0, 125.0, 1000.0, 8000000000000.0)
    val df = data.zip(expected).toSeq.toDF("price", "expectedPrice")

    val featurizer = new PowerTransformFeaturizer()
      .setInputCol("price")
      .setOutputCol("transformedPrice")
      .setPowerType(3)

    testTransformer[(Double, Double)](df, featurizer, "transformedPrice", "expectedPrice")(
      assertResult)
  }


  test("Power -2 Power Transform") {
    val data = Seq(1, 2, 3, 4, 5, 10, -5)
    val expected = Seq(1.0, 0.25, 0.11111, 0.0625, 0.04, 0.01, 0.04)
    val df = data.zip(expected).toSeq.toDF("price", "expectedPrice")

    val featurizer = new PowerTransformFeaturizer()
      .setInputCol("price")
      .setOutputCol("transformedPrice")
      .setPowerType(-2)

    testTransformer[(Double, Double)](df, featurizer, "transformedPrice", "expectedPrice")(
      assertResult)
  }

  test("PowerTransformFeaturizer read/write") {
    val t = new PowerTransformFeaturizer()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setPowerType(2)
    testDefaultReadWrite(t)
  }
}

