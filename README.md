# ML Featurizer
Feature engineering is a difficult and time consuming process. ML Featurizer is a library
to enable users to create additional features from raw data with ease. 
It extends and enriches the existing [Spark's feature engineering functionality](https://spark.apache.org/docs/latest/ml-features.html).

#### Featurizers provided by the library

  1. Unary Temporal Featurizers
      * DayOfWeekFeaturizer
      * HourOfDayFeaturizer
      * MonthOfYearFeaturizer
      * PartsOfDayFeaturizer
      * WeekendFeaturizer
  2. Unary Numeric Featurizers
      * LogTransformFeaturizer
      * MathFeaturizer
      * PowerTransformFeaturizer
  3. Binary Temporal Featurizers
      * DateDiffFeaturizer
  4. Binary Numeric Featurizers
      * AdditionFeaturizer
      * DivisionFeaturizer
      * MultiplicationFeaturizer
      * SubtractionFeaturizer
  5. Binary String Featurizers
      * ConcateColumnsFeaturizer
  6. Grouping Featurizers
      * GroupByFeaturizer (count, ratio, min, max, count, avg, sum)
  7. GEO Featurizers
      * GeohashFeaturizer
#### Examples:
##### Create day of week feature
```scala
object DayOfWeekFeaturizerExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DayOfWeekFeaturizer").master("local").getOrCreate()

    val data = Array((0, "2018-01-02"),
      (1, "2018-02-02"),
      (2, "2018-03-02"),
      (3, "2018-04-05"),
      (3, "2018-05-05"))
    val dataFrame = spark.createDataFrame(data).toDF("id", "date")

    val featurizer = new DayOfWeekFeaturizer()
      .setInputCol("date")
      .setOutputCol("dayOfWeek")
      .setFormat("yyyy-MM-dd")

    val featurizedDataFrame = featurizer.transform(dataFrame)
    featurizedDataFrame.show()
  }
}

```      
##### Use featurizers in Spark ML Pipeline

```scala
object FeaturePipeline {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("FeaturePipeline").master("local").getOrCreate()

    val data = Array((0, "2018-01-02", 1.0, 2.0, "mercedes"),
      (1, "2018-02-02", 2.5, 3.5, "lexus"),
      (2, "2018-03-02", 5.0, 1.0, "toyota"),
      (3, "2018-04-05", 8.0, 9.0, "tesla"),
      (4, "2018-05-05", 1.0, 5.0, "bmw"),
      (4, "2018-05-05", 1.0, 5.0, "bmw"))
    val dataFrame = spark.createDataFrame(data).toDF("id", "date", "price1", "price2", "brand")

    val dayOfWeekfeaturizer = new DayOfWeekFeaturizer()
      .setInputCol("date")
      .setOutputCol("dayOfWeek")
      .setFormat("yyyy-MM-dd")

    val monthOfYearfeaturizer = new MonthOfYearFeaturizer()
      .setInputCol("date")
      .setOutputCol("monthOfYear")
      .setFormat("yyyy-MM-dd")

    val weekendFeaturizer = new WeekendFeaturizer()
      .setInputCol("date")
      .setOutputCol("isWeekend")
      .setFormat("yyyy-MM-dd")

    val additionFeaturizer = new AdditionFeaturizer()
      .setInputCols("price1", "price2")
      .setOutputCol("price1_add_price2")

    val indexer = new StringIndexer()
      .setInputCol("brand")
      .setOutputCol("brandIndex")

    val encoder = new OneHotEncoder()
      .setInputCol("brandIndex")
      .setOutputCol("brandVector")

    val pipeline = new Pipeline()
      .setStages(Array(dayOfWeekfeaturizer, monthOfYearfeaturizer, weekendFeaturizer, additionFeaturizer,
        indexer, encoder))
    val model = pipeline.fit(dataFrame)
    model.transform(dataFrame).show()
  }
}
```
#### References:
* [An Empirical Analysis of Feature Engineering for
 Predictive Modeling](https://arxiv.org/pdf/1701.07852.pdf)      

### Contributing

If you're interested in contributing to this project, check out our [contribution guidelines](CONTRIBUTING.md)!
