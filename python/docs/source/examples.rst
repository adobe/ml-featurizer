ML Featurizer Examples
======================

Unary Numeric Featurizers
-------------------------

LogTransformFeaturizer
^^^^^^^^^^^^^^^^^^^^^^

.. code:: python3

   import mlfeaturizer.core.featurizer as fr
   data = spark.createDataFrame([
   (0, 200000, 2.5), 
   (1, 10000, 5.0), 
   (2, 150000, 5.0)], ["id", "v1", "v2"])

   logFeaturizer = fr.LogTransformFeaturizer(inputCol='v1', outputCol='output', logType='log10')
   logFeaturizer.transform(data).show()


PowerTransformFeaturizer
^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: python3

   import mlfeaturizer.core.featurizer as fr
   data = spark.createDataFrame([
   (0, 2.0, 2.5), 
   (1, 100.0, 5.0), 
   (2, 3.0, 5.0)], ["id", "v1", "v2"])

   powerFeaturizer = fr.PowerTransformFeaturizer(inputCol='v1', outputCol='output', powerType=2)
   powerFeaturizer.transform(data).show()


MathFeaturizer
^^^^^^^^^^^^^^

.. code:: python3

   import mlfeaturizer.core.featurizer as fr
   data = spark.createDataFrame([
   (0, 25.0, 2.5), 
   (1, 100.0, 5.0), 
   (2, 35.0, 5.0)], ["id", "v1", "v2"])

   mathFeaturizer = fr.MathFeaturizer(inputCol='v1', outputCol='output', mathFunction='sqrt')
   mathFeaturizer.transform(data).show()


Binary Numeric Featurizers
--------------------------

AdditionFeaturizer
^^^^^^^^^^^^^^^^^^

.. code:: python3

   import mlfeaturizer.core.featurizer as fr
   data = spark.createDataFrame([
   (0, 2.5, 2.5), 
   (1, 8.0, 5.0), 
   (2, 1.0, 5.0)], ["id", "v1", "v2"])

   addFeaturizer = fr.AdditionFeaturizer(inputCols=['v1', 'v2'], outputCol='output')
   addFeaturizer.transform(data).show()

SubtractionFeaturizer
^^^^^^^^^^^^^^^^^^^^^

.. code:: python3

   import mlfeaturizer.core.featurizer as fr
   data = spark.createDataFrame([
   (0, 2.5, 2.5), 
   (1, 8.0, 5.0), 
   (2, 1.0, 5.0)], ["id", "v1", "v2"])

   subtractFeaturizer = fr.SubtractionFeaturizer(inputCols=['v1', 'v2'], outputCol='output')
   subtractFeaturizer.transform(data).show()


MultiplicationFeaturizer
^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: python3

   import mlfeaturizer.core.featurizer as fr
   data = spark.createDataFrame([
   (0, 2.5, 2.5), 
   (1, 8.0, 5.0), 
   (2, 1.0, 5.0)], ["id", "v1", "v2"])

   multiplyFeaturizer = fr.MultiplicationFeaturizer(inputCols=['v1', 'v2'], outputCol='output')
   multiplyFeaturizer.transform(data).show()


DivisionFeaturizer
^^^^^^^^^^^^^^^^^^

.. code:: python3

   import mlfeaturizer.core.featurizer as fr
   data = spark.createDataFrame([
   (0, 2.5, 2.5), 
   (1, 8.0, 5.0), 
   (2, 1.0, 5.0)], ["id", "v1", "v2"])

   divideFeaturizer = fr.DivisionFeaturizer(inputCols=['v1', 'v2'], outputCol='output')
   divideFeaturizer.transform(data).show()


Unary Temporal Featurizers
--------------------------

HourOfDayFeaturizer
^^^^^^^^^^^^^^^^^^^

.. code:: python3

   import mlfeaturizer.core.featurizer as fr
   data = spark.createDataFrame([
   (0, 2.0, '2018-01-01 12:05:00'),
   (1, 100.0, '2018-12-01 08:05:00'),
   (2, 3.0, '2015-05-01 23:05:00')], ["id", "v1", "time"])

   hourFeaturizer = fr.HourOfDayFeaturizer(inputCol='time', outputCol='hour')
   hourFeaturizer.transform(data).show()


DayOfWeekFeaturizer
^^^^^^^^^^^^^^^^^^^

.. code:: python3

   import mlfeaturizer.core.featurizer as fr
   data = spark.createDataFrame([
   (0, 2.0, '2018-01-01 12:05:00'),
   (1, 100.0, '2018-12-01 08:05:00'),
   (2, 3.0, '2015-05-01 23:05:00')], ["id", "v1", "time"])

   dayFeaturizer = fr.DayOfWeekFeaturizer(inputCol='time', outputCol='day', format='yyyy-MM-dd HH:mm:ss')
   dayFeaturizer.transform(data).show()


MonthOfYearFeaturizer
^^^^^^^^^^^^^^^^^^^^^

.. code:: python3

   import mlfeaturizer.core.featurizer as fr
   data = spark.createDataFrame([
   (0, 2.0, '2018-01-01 12:05:00'),
   (1, 100.0, '2018-12-01 08:05:00'),
   (2, 3.0, '2015-05-01 23:05:00')], ["id", "v1", "time"])

   monthFeaturizer = fr.MonthOfYearFeaturizer(inputCol='time', outputCol='month', format='yyyy-MM-dd HH:mm:ss')
   monthFeaturizer.transform(data).show()


PartsOfDayFeaturizer
^^^^^^^^^^^^^^^^^^^^

.. code:: python3

   import mlfeaturizer.core.featurizer as fr
   data = spark.createDataFrame([
   (0, 2.0, '2018-01-01 12:05:00'),
   (1, 100.0, '2018-12-01 08:05:00'),
   (2, 3.0, '2015-05-01 23:05:00')], ["id", "v1", "time"])

   dayPartFeaturizer = fr.PartsOfDayFeaturizer(inputCol='time', outputCol='dayPart', format='yyyy-MM-dd HH:mm:ss')
   dayPartFeaturizer.transform(data).show()


WeekendFeaturizer
^^^^^^^^^^^^^^^^^

.. code:: python3

   import mlfeaturizer.core.featurizer as fr
   data = spark.createDataFrame([
   (0, 2.0, '2018-01-01 12:05:00'),
   (1, 100.0, '2018-12-01 08:05:00'),
   (2, 3.0, '2015-05-01 23:05:00')], ["id", "v1", "time"])

   weekendFeaturizer = fr.WeekendFeaturizer(inputCol='time', outputCol='weekend', format='yyyy-MM-dd HH:mm:ss')
   weekendFeaturizer.transform(data).show()


Grouping Featurizer
-------------------

Group by Sum
^^^^^^^^^^^^

.. code:: python3

   import mlfeaturizer.core.featurizer as fr
   data = spark.createDataFrame([
   (0, 250.0, '2018-01-01 12:05:00'),
   (0, 350.0, '2018-01-03 15:15:00'),
   (1, 150.0, '2018-12-01 08:05:00'),
   (1, 580.0, '2018-12-02 20:15:00'),
   (1, 850.0, '2018-12-03 20:15:00'),
   (2, 30.0, '2015-05-01 23:05:00')], ["id", "amountSpent", "time"])

   groupByFeaturizer = fr.GroupByFeaturizer(inputCol='id', aggregateCol='amountSpent', aggregateType='sum', outputCol='sumByGroup')
   groupByFeaturizer.transform(data).show()


Group by Min
^^^^^^^^^^^^

.. code:: python3

   import mlfeaturizer.core.featurizer as fr
   data = spark.createDataFrame([
   (0, 250.0, '2018-01-01 12:05:00'),
   (0, 350.0, '2018-01-03 15:15:00'),
   (1, 150.0, '2018-12-01 08:05:00'),
   (1, 580.0, '2018-12-02 20:15:00'),
   (1, 850.0, '2018-12-03 20:15:00'),
   (2, 30.0, '2015-05-01 23:05:00')], ["id", "amountSpent", "time"])

   groupByFeaturizer = fr.GroupByFeaturizer(inputCol='id', aggregateCol='amountSpent', aggregateType='min', outputCol='minByGroup')
   groupByFeaturizer.transform(data).show()


Group by Max
^^^^^^^^^^^^

.. code:: python3

   import mlfeaturizer.core.featurizer as fr
   data = spark.createDataFrame([
   (0, 250.0, '2018-01-01 12:05:00'),
   (0, 350.0, '2018-01-03 15:15:00'),
   (1, 150.0, '2018-12-01 08:05:00'),
   (1, 580.0, '2018-12-02 20:15:00'),
   (1, 850.0, '2018-12-03 20:15:00'),
   (2, 30.0, '2015-05-01 23:05:00')], ["id", "amountSpent", "time"])

   groupByFeaturizer = fr.GroupByFeaturizer(inputCol='id', aggregateCol='amountSpent', aggregateType='max', outputCol='maxByGroup')
   groupByFeaturizer.transform(data).show()


Group by Average
^^^^^^^^^^^^^^^^

.. code:: python3

   import mlfeaturizer.core.featurizer as fr
   data = spark.createDataFrame([
   (0, 250.0, '2018-01-01 12:05:00'),
   (0, 350.0, '2018-01-03 15:15:00'),
   (1, 150.0, '2018-12-01 08:05:00'),
   (1, 580.0, '2018-12-02 20:15:00'),
   (1, 850.0, '2018-12-03 20:15:00'),
   (2, 30.0, '2015-05-01 23:05:00')], ["id", "amountSpent", "time"])

   groupByFeaturizer = fr.GroupByFeaturizer(inputCol='id', aggregateCol='amountSpent', aggregateType='avg', outputCol='avgByGroup')
   groupByFeaturizer.transform(data).show() 


 Group by Count
^^^^^^^^^^^^^^^

.. code:: python3

   import mlfeaturizer.core.featurizer as fr
   data = spark.createDataFrame([
   (0, 250.0, '2018-01-01 12:05:00'),
   (0, 350.0, '2018-01-03 15:15:00'),
   (1, 150.0, '2018-12-01 08:05:00'),
   (1, 580.0, '2018-12-02 20:15:00'),
   (1, 850.0, '2018-12-03 20:15:00'),
   (2, 30.0, '2015-05-01 23:05:00')], ["id", "amountSpent", "time"])

   groupByFeaturizer = fr.GroupByFeaturizer(inputCol='id', aggregateCol='amountSpent', aggregateType='count', outputCol='countByGroup')
   groupByFeaturizer.transform(data).show()     

