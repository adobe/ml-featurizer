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
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}

class AdditionFeaturizerSuite extends MLTest with DefaultReadWriteTest {
  import testImplicits._

  test("AdditionFeaturizer params") {
    ParamsSuite.checkParams(new AdditionFeaturizer())
  }

  test("Addition of two columns") {
    val original = Seq((0, 1.0, 3.0), (2, 2.0, 5.0),(3, -1.0, 2.0), (5, -5.0, -8.0),
      (8, 100.0, 200.0)).toDF("id", "v1", "v2")
    val featurizer = new AdditionFeaturizer().setInputCols("v1", "v2").setOutputCol("v3")
    val expected = Seq((0, 1.0, 3.0, 4.0), (2, 2.0, 5.0, 7.0), (3, -1.0, 2.0, 1.0),
      (5, -5.0, -8.0, -13.0), (8, 100.0, 200.0, 300.0)).toDF("id", "v1", "v2", "v3")
    val resultSchema = featurizer.transformSchema(original.schema)
    testTransformerByGlobalCheckFunc[(Int, Double, Double)](
      original,
      featurizer,
      "id",
      "v1",
      "v2",
      "v3") { rows =>
      assert(rows.head.schema.toString == resultSchema.toString)
      assert(resultSchema == expected.schema)
      assert(rows == expected.collect().toSeq)
    }
  }

  test("AdditionFeaturizer read/write") {
    val t = new AdditionFeaturizer()
      .setInputCols("myInputCol1","myInputCol2")
      .setOutputCol("myOutputCol")
    testDefaultReadWrite(t)
  }
}
