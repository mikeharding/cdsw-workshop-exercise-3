from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from numpy import array

# Boilerplate Spark stuff:
spark = SparkSession\
    .builder\
    .appName("diabetes_patient_readmission_model")\
    .getOrCreate()
sc = spark.sparkContext



#Load up our CSV file, and filter out the header line with the column names
dataframe = spark.table("diabetic_data_original_parquet")
modelPath = "hdfs:///tmp/diabetes/diabetic_data_model/"



#get  all data
print "All data %d", dataframe.count()


# # Feature Extraction and Model Training
#
# We need to:
# * Code features that are not already numeric
# * Gather all features we need into a single column in the DataFrame.
# * Split labeled data into training and testing set
# * Fit the model to the training data.

# ## Note
# StringIndexer --> Turns string value into numerical value. I.E. below we use it to map "churned" ("yes" or "no") into numerical values and put in it colum "label".
#
# VectorAssembler --> takes input columns and returns a vector object into output column

numeric_cols = ["time_in_hospital", "num_lab_procedures", "num_procedures",
                        "num_medications", "number_outpatient", "number_inpatient",
                        "number_diagnoses"]
string_cols =['A1Cresult_indexed', 'insulin_indexed','diabetesMed_indexed']

from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler

label_indexer = StringIndexer(inputCol = 'isreadmitted', outputCol = 'label')

A1Cresult_indexer = StringIndexer(inputCol = 'A1Cresult', outputCol = 'A1Cresult_indexed')
insulin_indexer = StringIndexer(inputCol = 'insulin', outputCol = 'insulin_indexed')
diabetesMed_indexer = StringIndexer(inputCol = 'diabetesMed', outputCol = 'diabetesMed_indexed')




assembler = VectorAssembler(
    inputCols =  string_cols+ numeric_cols,
    outputCol = 'features')


# # Model Training
#
# We can now define our classifier and pipeline. With this done, we can split our labeled data in train and test sets and fit a model.
#
# To train the decision tree, give it the feature vector column and the label column.
#
# Pipeline is defined by stages. Index plan column, label column, create vectors, then define the decision tree.


from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier

classifier = DecisionTreeClassifier(labelCol = 'label', featuresCol = 'features')

pipeline = Pipeline(stages=[A1Cresult_indexer,insulin_indexer,diabetesMed_indexer,
                            label_indexer, assembler, classifier])

(train, test) = dataframe.randomSplit([0.7, 0.3])
model = pipeline.fit(train)


# ## Model Evaluation
#



from pyspark.ml.evaluation import BinaryClassificationEvaluator

predictions = model.transform(test)
evaluator = BinaryClassificationEvaluator()
auroc = evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})
"The AUROC is %s " % (auroc)



# # Fit a RandomForestClassifier
#
# Fit a random forest classifier to the data. Try experimenting with different values of the `maxDepth`, `numTrees`, and `entropy` parameters to see which gives the best classification performance. Do the settings that give the best classification performance on the training set also give the best classification performance on the test set?
#


from pyspark.ml.classification import RandomForestClassifier

classifier = RandomForestClassifier(labelCol = 'label', featuresCol = 'features', numTrees= 10)
pipeline = Pipeline(stages=[A1Cresult_indexer, insulin_indexer,diabetesMed_indexer,
                            label_indexer, assembler, classifier])
model = pipeline.fit(train)

predictions = model.transform(test)
evaluator = BinaryClassificationEvaluator()
auroc = evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})
"The AUROC is %s " % (auroc)


predictions.select('label','prediction','probability').filter('prediction = 1').show(50)

#Let us save the model
model.write().overwrite().save(modelPath)

from pyspark.ml import PipelineModel
sameModel = PipelineModel.load(modelPath)
predictions = sameModel.transform(test)
predictions.select('label','prediction','probability').filter('prediction = 1').show(50)



test.filter(test.isreadmitted==1).show(1)

print("Model saved:\n")






spark.stop()
