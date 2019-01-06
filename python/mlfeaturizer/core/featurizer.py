from pyspark.ml import feature, Pipeline
from pyspark import since, keyword_only, SparkContext
from pyspark.rdd import ignore_unicode_prefix
from pyspark.ml.linalg import _convert_to_vector
from pyspark.ml.param.shared import *
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaEstimator, JavaModel, JavaParams, JavaTransformer, _jvm
from pyspark.ml.common import inherit_doc

__all__ = ['LogTransformFeaturizer', 'PowerTransformFeaturizer',
           'DayOfWeekFeaturizer', 'HourOfDayFeaturizer',
           'MonthOfYearFeaturizer', 'PartsOfDayFeaturizer',
           'AdditionFeaturizer', 'SubtractionFeaturizer',
           'MultiplicationFeaturizer', 'DivisionFeaturizer']

@inherit_doc
class LogTransformFeaturizer(JavaTransformer, HasInputCol, HasOutputCol,
                 JavaMLReadable, JavaMLWritable):
    """
    Perform Log Transformation on column.
    """

    logType = Param(Params._dummy(), "logType", "log type to be used. " +
                          "Options are 'natural' (natural log), " +
                          "'log10' (log base 10), or 'log2' (log base 2).",
                          typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, logType="natural"):
        """
        __init__(self, inputCol=None, outputCol=None, logType="natural")
        """
        super(LogTransformFeaturizer, self).__init__()
        self._java_obj = self._new_java_obj("com.adobe.platform.ml.feature.unary.numeric.LogTransformFeaturizer",
                                            self.uid)
        self._setDefault(logType="natural")
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, inputCol=None, outputCol=None, logType="natural"):
        """
        setParams(self, inputCol=None, outputCol=None, logType="natural")
        Sets params for this LogTransformFeaturizer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setLogType(self, value):
        """
        Sets the value of :py:attr:`logType`.
        """
        return self._set(logType=value)

    @since("1.4.0")
    def getLogType(self):
        """
        Gets the value of logType or its default value.
        """
        return self.getOrDefault(self.logType)

@inherit_doc
class PowerTransformFeaturizer(JavaTransformer, HasInputCol, HasOutputCol,
                 JavaMLReadable, JavaMLWritable):
    """
    Perform Power Transformation on column.
    """

    powerType = Param(Params._dummy(), "powerType", "power type to be used. " +
                          "Any integer greater than 0. Default is power of 2",
                          typeConverter=TypeConverters.toInt)

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, powerType=2):
        """
        __init__(self, inputCol=None, outputCol=None, powerType=2)
        """
        super(PowerTransformFeaturizer, self).__init__()
        self._java_obj = self._new_java_obj("com.adobe.platform.ml.feature.unary.numeric.PowerTransformFeaturizer",
                                            self.uid)
        self._setDefault(powerType=2)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, inputCol=None, outputCol=None, powerType=2):
        """
        setParams(self, inputCol=None, outputCol=None, powerType=2)
        Sets params for this PowerTransformFeaturizer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setPowerType(self, value):
        """
        Sets the value of :py:attr:`powerType`.
        """
        return self._set(powerType=value)

    @since("1.4.0")
    def getPowerType(self):
        """
        Gets the value of logType or its default value.
        """
        return self.getOrDefault(self.powerType)


@inherit_doc
class DayOfWeekFeaturizer(JavaTransformer, HasInputCol, HasOutputCol,
                 JavaMLReadable, JavaMLWritable):
    """
    Convert date time to day of week.
    """

    format = Param(Params._dummy(), "format", "specify timestamp pattern. ",
                          typeConverter=TypeConverters.toString)
    timezone = Param(Params._dummy(), "timezone", "specify timezone. ",
                   typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, format="yyyy-MM-dd", timezone="UTC"):
        """
        __init__(self, inputCol=None, outputCol=None, format="yyyy-MM-dd", timezone="UTC")
        """
        super(DayOfWeekFeaturizer, self).__init__()
        self._java_obj = self._new_java_obj("com.adobe.platform.ml.feature.unary.temporal.DayOfWeekFeaturizer",
                                            self.uid)
        self._setDefault(format="yyyy-MM-dd", timezone="UTC")
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, inputCol=None, outputCol=None, format="yyyy-MM-dd", timezone="UTC"):
        """
        setParams(self, inputCol=None, outputCol=None, format="yyyy-MM-dd", timezone="UTC")
        Sets params for this DayOfWeekFeaturizer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setFormat(self, value):
        """
        Sets the value of :py:attr:`format`.
        """
        return self._set(format=value)

    @since("1.4.0")
    def getFormat(self):
        """
        Gets the value of format or its default value.
        """
        return self.getOrDefault(self.format)

    @since("1.4.0")
    def setTimezone(self, value):
        """
        Sets the value of :py:attr:`timezone`.
        """
        return self._set(timezone=value)

    @since("1.4.0")
    def getTimezone(self):
        """
        Gets the value of timezone or its default value.
        """
        return self.getOrDefault(self.timezone)


@inherit_doc
class HourOfDayFeaturizer(JavaTransformer, HasInputCol, HasOutputCol,
                 JavaMLReadable, JavaMLWritable):
    """
    Convert date time to hour of day.
    """

    format = Param(Params._dummy(), "format", "specify timestamp pattern. ",
                          typeConverter=TypeConverters.toString)
    timezone = Param(Params._dummy(), "timezone", "specify timezone. ",
                   typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, format="yyyy-MM-dd HH:mm:ss", timezone="UTC"):
        """
        __init__(self, inputCol=None, outputCol=None, format="yyyy-MM-dd HH:mm:ss", timezone="UTC")
        """
        super(HourOfDayFeaturizer, self).__init__()
        self._java_obj = self._new_java_obj("com.adobe.platform.ml.feature.unary.temporal.HourOfDayFeaturizer",
                                            self.uid)
        self._setDefault(format="yyyy-MM-dd HH:mm:ss", timezone="UTC")
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, inputCol=None, outputCol=None, format="yyyy-MM-dd HH:mm:ss", timezone="UTC"):
        """
        setParams(self, inputCol=None, outputCol=None, format="yyyy-MM-dd HH:mm:ss", timezone="UTC")
        Sets params for this HourOfDayFeaturizer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setFormat(self, value):
        """
        Sets the value of :py:attr:`format`.
        """
        return self._set(format=value)

    @since("1.4.0")
    def getFormat(self):
        """
        Gets the value of format or its default value.
        """
        return self.getOrDefault(self.format)

    @since("1.4.0")
    def setTimezone(self, value):
        """
        Sets the value of :py:attr:`timezone`.
        """
        return self._set(timezone=value)

    @since("1.4.0")
    def getTimezone(self):
        """
        Gets the value of timezone or its default value.
        """
        return self.getOrDefault(self.timezone)

@inherit_doc
class MonthOfYearFeaturizer(JavaTransformer, HasInputCol, HasOutputCol,
                 JavaMLReadable, JavaMLWritable):
    """
    Convert date time to month of year.
    """

    format = Param(Params._dummy(), "format", "specify timestamp pattern. ",
                          typeConverter=TypeConverters.toString)
    timezone = Param(Params._dummy(), "timezone", "specify timezone. ",
                   typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, format="yyyy-MM-dd", timezone="UTC"):
        """
        __init__(self, inputCol=None, outputCol=None, format="yyyy-MM-dd", timezone="UTC")
        """
        super(MonthOfYearFeaturizer, self).__init__()
        self._java_obj = self._new_java_obj("com.adobe.platform.ml.feature.unary.temporal.MonthOfYearFeaturizer",
                                            self.uid)
        self._setDefault(format="yyyy-MM-dd", timezone="UTC")
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, inputCol=None, outputCol=None, format="yyyy-MM-dd", timezone="UTC"):
        """
        setParams(self, inputCol=None, outputCol=None, format="yyyy-MM-dd", timezone="UTC")
        Sets params for this MonthOfYearFeaturizer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setFormat(self, value):
        """
        Sets the value of :py:attr:`format`.
        """
        return self._set(format=value)

    @since("1.4.0")
    def getFormat(self):
        """
        Gets the value of format or its default value.
        """
        return self.getOrDefault(self.format)

    @since("1.4.0")
    def setTimezone(self, value):
        """
        Sets the value of :py:attr:`timezone`.
        """
        return self._set(timezone=value)

    @since("1.4.0")
    def getTimezone(self):
        """
        Gets the value of timezone or its default value.
        """
        return self.getOrDefault(self.timezone)

@inherit_doc
class PartsOfDayFeaturizer(JavaTransformer, HasInputCol, HasOutputCol,
                 JavaMLReadable, JavaMLWritable):
    """
    Convert date time to parts of day.
    """

    format = Param(Params._dummy(), "format", "specify timestamp pattern. ",
                          typeConverter=TypeConverters.toString)
    timezone = Param(Params._dummy(), "timezone", "specify timezone. ",
                   typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, format="yyyy-MM-dd HH:mm:ss", timezone="UTC"):
        """
        __init__(self, inputCol=None, outputCol=None, format="yyyy-MM-dd HH:mm:ss", timezone="UTC")
        """
        super(PartsOfDayFeaturizer, self).__init__()
        self._java_obj = self._new_java_obj("com.adobe.platform.ml.feature.unary.temporal.PartsOfDayFeaturizer",
                                            self.uid)
        self._setDefault(format="yyyy-MM-dd HH:mm:ss", timezone="UTC")
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, inputCol=None, outputCol=None, format="yyyy-MM-dd HH:mm:ss", timezone="UTC"):
        """
        setParams(self, inputCol=None, outputCol=None, format="yyyy-MM-dd HH:mm:ss", timezone="UTC")
        Sets params for this PartsOfDayFeaturizer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setFormat(self, value):
        """
        Sets the value of :py:attr:`format`.
        """
        return self._set(format=value)

    @since("1.4.0")
    def getFormat(self):
        """
        Gets the value of format or its default value.
        """
        return self.getOrDefault(self.format)

    @since("1.4.0")
    def setTimezone(self, value):
        """
        Sets the value of :py:attr:`timezone`.
        """
        return self._set(timezone=value)

    @since("1.4.0")
    def getTimezone(self):
        """
        Gets the value of timezone or its default value.
        """
        return self.getOrDefault(self.timezone)

@inherit_doc
class AdditionFeaturizer(JavaTransformer, HasInputCols, HasOutputCol,
                 JavaMLReadable, JavaMLWritable):
    """
    Add two numeric columns.
    """

    @keyword_only
    def __init__(self, inputCols=None, outputCol=None):
        """
        __init__(self, inputCols=None, outputCol=None)
        """
        super(AdditionFeaturizer, self).__init__()
        self._java_obj = self._new_java_obj("com.adobe.platform.ml.feature.binary.numeric.AdditionFeaturizer",
                                            self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, inputCols=None, outputCol=None):
        """
        setParams(self, inputCols=None, outputCol=None)
        Sets params for this AdditionFeaturizer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

@inherit_doc
class SubtractionFeaturizer(JavaTransformer, HasInputCols, HasOutputCol,
                 JavaMLReadable, JavaMLWritable):
    """
    Subtract two numeric columns.
    """

    @keyword_only
    def __init__(self, inputCols=None, outputCol=None):
        """
        __init__(self, inputCols=None, outputCol=None)
        """
        super(SubtractionFeaturizer, self).__init__()
        self._java_obj = self._new_java_obj("com.adobe.platform.ml.feature.binary.numeric.SubtractionFeaturizer",
                                            self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, inputCols=None, outputCol=None):
        """
        setParams(self, inputCols=None, outputCol=None)
        Sets params for this SubtractionFeaturizer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

@inherit_doc
class MultiplicationFeaturizer(JavaTransformer, HasInputCols, HasOutputCol,
                 JavaMLReadable, JavaMLWritable):
    """
    Multiply two numeric columns.
    """

    @keyword_only
    def __init__(self, inputCols=None, outputCol=None):
        """
        __init__(self, inputCols=None, outputCol=None)
        """
        super(MultiplicationFeaturizer, self).__init__()
        self._java_obj = self._new_java_obj("com.adobe.platform.ml.feature.binary.numeric.MultiplicationFeaturizer",
                                            self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, inputCols=None, outputCol=None):
        """
        setParams(self, inputCols=None, outputCol=None)
        Sets params for this MultiplicationFeaturizer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

@inherit_doc
class DivisionFeaturizer(JavaTransformer, HasInputCols, HasOutputCol,
                 JavaMLReadable, JavaMLWritable):
    """
    Divide two numeric columns.
    """

    @keyword_only
    def __init__(self, inputCols=None, outputCol=None):
        """
        __init__(self, inputCols=None, outputCol=None)
        """
        super(DivisionFeaturizer, self).__init__()
        self._java_obj = self._new_java_obj("com.adobe.platform.ml.feature.binary.numeric.DivisionFeaturizer",
                                            self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, inputCols=None, outputCol=None):
        """
        setParams(self, inputCols=None, outputCol=None)
        Sets params for this DivisionFeaturizer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)