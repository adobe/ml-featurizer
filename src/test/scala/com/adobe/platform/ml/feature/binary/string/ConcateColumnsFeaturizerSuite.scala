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

package com.adobe.platform.ml.feature.binary.string

import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}

class ConcateColumnsFeaturizerSuite extends MLTest with DefaultReadWriteTest {
  import testImplicits._

  def threeColumnsTestWithDelimiter(delimiter:String):(ConcateColumnsFeaturizer, Seq[Row]) = {
    val featurizer = new ConcateColumnsFeaturizer().setInputCols("gender", "income", "marital")
      .setOutputCol("gender_income_marital").setDelimiter(delimiter)
    val expected = Seq(Row(0, "F", "High", "married", s"F${delimiter}High${delimiter}married"),
      Row(2, "M", "High", "married", s"M${delimiter}High${delimiter}married"),
      Row(3, "M", "Low", "single", s"M${delimiter}Low${delimiter}single"),
      Row(5, "F", "High", "single", s"F${delimiter}High${delimiter}single"),
      Row(8, "F", "High", "married", s"F${delimiter}High${delimiter}married"))
    (featurizer,expected)
  }

  test("ConcateColumnsFeaturizer params") {
    ParamsSuite.checkParams(new ConcateColumnsFeaturizer())
  }

  test("Concatenation of two columns") {
    val original = Seq((0, "F", "High"), (2, "M", "High"),(3, "M", "Low"), (5, "F", "High"),
      (8, "F", "High")).toDF("id", "gender", "income")
    val featurizer = new ConcateColumnsFeaturizer().setInputCols("gender", "income").setOutputCol("gender_income")
    val expected = Seq(Row(0, "F", "High", "F,High"), Row(2, "M", "High", "M,High"),Row(3, "M", "Low", "M,Low"),
      Row(5, "F", "High", "F,High"),
      Row(8, "F", "High", "F,High"))

    val schema = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("gender", StringType, true),
      StructField("income", StringType, true),
      StructField("gender_income", StringType, false)
    ))

    val rdd = spark.sparkContext.parallelize(expected)
    val df = spark.createDataFrame(rdd, schema)

    val resultSchema = featurizer.transformSchema(original.schema)
    testTransformerByGlobalCheckFunc[(Int, String, String)](
      original,
      featurizer,
      "id",
      "gender",
      "income",
      "gender_income") { rows =>
      assert(rows.head.schema.toString == resultSchema.toString)
      assert(resultSchema == df.schema)
      assert(rows == expected.toSeq)
    }
  }

  test("Concatenation of three columns with comma delimiter") {
    val original = Seq((0, "F", "High", "married"),
      (2, "M", "High", "married"),
      (3, "M", "Low", "single"),
      (5, "F", "High", "single"),
      (8, "F", "High", "married")).toDF("id", "gender", "income", "marital")

    val (featurizer, expected) = threeColumnsTestWithDelimiter(",")

    val schema = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("gender", StringType, true),
      StructField("income", StringType, true),
      StructField("marital", StringType, true),
      StructField("gender_income_marital", StringType, false)
    ))

    val rdd = spark.sparkContext.parallelize(expected)
    val df = spark.createDataFrame(rdd, schema)

    val resultSchema = featurizer.transformSchema(original.schema)
    testTransformerByGlobalCheckFunc[(Int, String, String, String)](
      original,
      featurizer,
      "id",
      "gender",
      "income",
      "marital",
      "gender_income_marital") { rows =>
      assert(rows.head.schema.toString == resultSchema.toString)
      assert(resultSchema == df.schema)
      assert(rows == expected.toSeq)
    }
  }

  test("Concatenation of three columns using dash delimiter") {
    val original = Seq((0, "F", "High", "married"),
      (2, "M", "High", "married"),
      (3, "M", "Low", "single"),
      (5, "F", "High", "single"),
      (8, "F", "High", "married")).toDF("id", "gender", "income", "marital")

    val (featurizer, expected) = threeColumnsTestWithDelimiter("-")

    val schema = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("gender", StringType, true),
      StructField("income", StringType, true),
      StructField("marital", StringType, true),
      StructField("gender_income_marital", StringType, false)
    ))

    val rdd = spark.sparkContext.parallelize(expected)
    val df = spark.createDataFrame(rdd, schema)

    val resultSchema = featurizer.transformSchema(original.schema)
    testTransformerByGlobalCheckFunc[(Int, String, String, String)](
      original,
      featurizer,
      "id",
      "gender",
      "income",
      "marital",
      "gender_income_marital") { rows =>
      assert(rows.head.schema.toString == resultSchema.toString)
      assert(resultSchema == df.schema)
      assert(rows == expected.toSeq)
    }
  }

  test("ConcateColumnsFeaturizer read/write") {
    val t = new ConcateColumnsFeaturizer()
      .setInputCols("myInputCol1","myInputCol2")
      .setOutputCol("myOutputCol")
      .setDelimiter("-")
    testDefaultReadWrite(t)
  }
}
