package com.adobe.platform.ml.feature.unary.numeric

import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.sql.Row
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}


class LogTransformFeaturizerSuite extends MLTest with DefaultReadWriteTest {
  import testImplicits._

  def assertResult: Row => Unit = {
    case Row(originalValue:Double, transformedValue:Double) =>
      assert(originalValue ~== transformedValue absTol 1E-5,
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
