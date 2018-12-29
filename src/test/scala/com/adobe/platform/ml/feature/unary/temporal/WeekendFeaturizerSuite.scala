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


class WeekendFeaturizerSuite extends MLTest with DefaultReadWriteTest {
   import testImplicits._

   def assertResult: Row => Unit = {
     case Row(transformedValue:Int, expectedValue:Int) =>
       assert(transformedValue === expectedValue,
         "The transformed value is not correct after weekend transform.")
   }

   test("params") {
     ParamsSuite.checkParams(new WeekendFeaturizer())
   }

   test("Default Weekend Transform") {
     val data = Seq("2018-08-01", "2018-05-26", "2018-08-19", "2019-08-18")
     val expected = Seq(0, 1, 1, 1)
     val df = data.zip(expected).toSeq.toDF("created", "expectedIsWeekend")

     val featurizer = new WeekendFeaturizer()
       .setInputCol("created")
       .setOutputCol("weekend")
       .setFormat("yyyy-MM-dd")

     testTransformer[(String, Int)](df, featurizer, "weekend", "expectedIsWeekend")(
       assertResult)
   }

   test("Weekend Transform with format yyyy-MM-dd HH:mm:ss") {
     val data = Seq("2018-08-01 23:00:00", "2018-05-26 00:00:00", "2018-08-19 08:00:00", "2019-08-18 18:00:00")
     val expected = Seq(0, 1, 1, 1)
     val df = data.zip(expected).toSeq.toDF("created", "expectedIsWeekend")

     val featurizer = new WeekendFeaturizer()
       .setInputCol("created")
       .setOutputCol("weekend")
       .setFormat("yyyy-MM-dd HH:mm:ss")

     testTransformer[(String, Int)](df, featurizer, "weekend", "expectedIsWeekend")(
       assertResult)
   }

   test("Weekend Transform with format yyyyMMddHHmmss") {
     val data = Seq("20180801230000", "20180526000000", "20180819080000", "20190818180000")
     val expected = Seq(0, 1, 1, 1)
     val df = data.zip(expected).toSeq.toDF("created", "expectedIsWeekend")

     val featurizer = new WeekendFeaturizer()
       .setInputCol("created")
       .setOutputCol("weekend")
       .setFormat("yyyyMMddHHmmss")
       .setTimezone("US/Central")

     testTransformer[(String, Int)](df, featurizer, "weekend", "expectedIsWeekend")(
       assertResult)
   }


   test("WeekendFeaturizer read/write") {
     val t = new WeekendFeaturizer()
       .setInputCol("myInputCol")
       .setOutputCol("myOutputCol")
       .setFormat("yyyyMMddHHmmss")
       .setTimezone("US/Central")
     testDefaultReadWrite(t)
   }
 }
