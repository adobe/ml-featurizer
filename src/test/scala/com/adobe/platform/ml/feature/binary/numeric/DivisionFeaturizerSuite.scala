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

package com.adobe.platform.ml.feature.binary.numeric

import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.sql.Row
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}


class DivisionFeaturizerSuite extends MLTest with DefaultReadWriteTest {
  import testImplicits._

  def assertResult: Row => Unit = {
    case Row(transformedValue:Double, expectedValue:Double) =>
      assert(transformedValue ~== expectedValue absTol 1E-5,
        "The transformed value is not correct after division.")
  }

  test("DivisonFeaturizer params") {
    ParamsSuite.checkParams(new DivisionFeaturizer())
  }

  test("Division of two columns") {
    val original = Seq((0, 4.0, 2.0, 2.0), (2, 15.0, 5.0, 3.0), (3, -5.0, 1.0, -5.0), (5, -9.0, -3.0, 3.0),
      (8, 200.0, 100.0, 2.0), (9, 200.0, 8.3, 24.09639), (10, 105.0, 3.9, 26.92308)).toDF("id", "v1", "v2", "expected")
    val featurizer = new DivisionFeaturizer().setInputCols("v1", "v2").setOutputCol("v3")
    testTransformerByGlobalCheckFunc[(Int, Double, Double, Double)](
      original,
      featurizer,
      "v3",
      "expected") {
      rows => rows.foreach(assertResult)
    }
  }

  test("DivisionFeaturizer read/write") {
    val t = new DivisionFeaturizer()
      .setInputCols("myInputCol1","myInputCol2")
      .setOutputCol("myOutputCol")
    testDefaultReadWrite(t)
  }
}
