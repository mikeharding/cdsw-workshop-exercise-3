from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sb
from pyspark.sql.types import *
from pyspark.sql.functions import desc

# Boilerplate Spark stuff:
spark = SparkSession\
    .builder\
    .appName("diabetes_patient_readmission_visualization")\
    .getOrCreate()



dataframe = spark.read.table("diabetic_data_original_parquet")
dataframe.printSchema()

#Encounters
print ('Total  Encounters :',dataframe.count())



# # Feature Visualization
#
# The data vizualization workflow for large data sets is usually:
#
# * Sample data so it fits in memory on a single machine.
# * Examine single variable distributions.
# * Examine joint distributions and correlations.
# * Look for other types of relationships.
#
# [DataFrame#sample() documentation](http://people.apache.org/~pwendell/spark-releases/spark-1.5.0-rc1-docs/api/python/pyspark.sql.html#pyspark.sql.DataFrame.sample)

sample_data = dataframe.sample(False, 0.5, 83).toPandas()
sample_data.transpose().head(100)

# Readmissions
dataframe.dropna().groupBy("isreadmitted").count().sort("isreadmitted").show()

pandas_readmitted_dataframe = dataframe.groupBy("isreadmitted").count().toPandas()

pandas_readmitted_dataframe.dropna().groupby(['isreadmitted'])[['count']].sum().plot(kind='bar')

readmitted_dataframe = dataframe.where(dataframe["isreadmitted"] == 1)

print ('Total  Readmissions :',readmitted_dataframe.count())

# Gender
dataframe.dropna().groupBy("gender").count().sort("gender").show()
pandas_gender_dataframe = readmitted_dataframe.groupBy("gender")\
                            .count().toPandas()

pandas_gender_dataframe.dropna()\
                .groupby(['gender'])[['count']].sum()\
                  .plot(kind='bar')

# Race
dataframe.dropna().groupBy("race").count().sort("race").show()
pandas_race_dataframe = readmitted_dataframe.groupBy("race")\
                            .count().toPandas()

pandas_race_dataframe.dropna()\
                .groupby(['race'])[['count']].sum()\
                  .plot(kind='bar')


# Top 10 readmissions
dataframe.groupby('patient_nbr').count().sort(desc('count')).show(10)


# #Feature Distributions
#
# We want to examine the distribution of our features, so start with them one at a time.
#
# Seaborn has a standard function called [dist()](http://stanford.edu/~mwaskom/software/seaborn/generated/seaborn.distplot.html#seaborn.distplot) that allows us to easily examine the distribution of a column of a pandas dataframe or a numpy array.
sample_data = dataframe.toPandas()
sb.distplot(sample_data["time_in_hospital"], kde=False)
sb.boxplot(x="isreadmitted", y="time_in_hospital", data=sample_data)




sb.distplot(sample_data["number_inpatient"], kde=False)
sb.boxplot(x="isreadmitted", y="number_inpatient", data=sample_data)

sb.distplot(sample_data["number_diagnoses"], kde=False)
sb.boxplot(x="isreadmitted", y="number_diagnoses", data=sample_data)


# # Joint Distributions
#
# Looking at joint distributions of data can also tell us a lot, particularly about redundant features. [Seaborn's PairPlot](http://stanford.edu/~mwaskom/software/seaborn/generated/seaborn.pairplot.html#seaborn.pairplot) let's us look at joint distributions for many variables at once.


numeric_data = sample_data[["time_in_hospital", "num_lab_procedures", "num_procedures", "isreadmitted"]]
sb.pairplot(numeric_data, hue="isreadmitted")


# visualize some strong linear relationships between some variables, let's get a general impression of the correlations between variables by using [Seaborn's heatmap functionality.](http://stanford.edu/~mwaskom/software/seaborn/generated/seaborn.heatmap.html)


corr = sample_data[["time_in_hospital", "num_lab_procedures", "num_procedures",
                    "num_medications", "number_outpatient", "number_inpatient",
                    "number_diagnoses"]].corr()

sb.heatmap(corr)



spark.stop()
