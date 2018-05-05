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

package com.adobe.platform.ml.feature.util

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param.{Param, Params, StringArrayParam}

/**
  * Find a way to extends from Spark. Since its from code generation, have some issues inherited from
  * Spark library.
  * Trait for shared param inputCol. This trait may be changed or
  * removed between minor versions.
  */
@DeveloperApi
trait HasInputCol extends Params {

  /**
    * Param for input column name.
    *
    * @group param
    */
  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")

  /** @group getParam */
  final def getInputCol: String = $(inputCol)
}

/**
  * Trait for shared param inputCols. This trait may be changed or
  * removed between minor versions.
  */
@DeveloperApi
trait HasInputCols extends Params {

  /**
    * Param for input column names.
    *
    * @group param
    */
  final val inputCols: StringArrayParam = new StringArrayParam(this, "inputCols", "input column names")

  /** @group getParam */
  final def getInputCols: Array[String] = $(inputCols)
}

/**
  * Trait for shared param outputCol (default: uid + "__output"). This trait may be changed or
  * removed between minor versions.
  **/
@DeveloperApi
trait HasOutputCol extends Params {

  /**
    * Param for output column name.
    *
    * @group param
    */
  final val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")

  setDefault(outputCol, uid + "__output")

  /** @group getParam */
  final def getOutputCol: String = $(outputCol)
}

/**
  * Trait for shared param outputCols. This trait may be changed or
  * removed between minor versions.
  */
@DeveloperApi
trait HasOutputCols extends Params {

  /**
    * Param for output column names.
    *
    * @group param
    */
  final val outputCols: StringArrayParam = new StringArrayParam(this, "outputCols", "output column names")

  /** @group getParam */
  final def getOutputCols: Array[String] = $(outputCols)
}

