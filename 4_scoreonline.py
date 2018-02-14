from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

# Boilerplate Spark stuff:
spark = SparkSession\
    .builder\
    .appName("diabetes_patient_readmission_onlinescore")\
    .getOrCreate()
sc = spark.sparkContext



#Load up patient discharge file, use the trained model and predict readmissions
test = spark.table("diabetic_data_original_parquet")
modelPath = "hdfs:///tmp/diabetes/diabetic_data_model/"


from pyspark.ml import PipelineModel
sameModel = PipelineModel.load(modelPath)
predictions = sameModel.transform(test)
predictions.select('encounter_id','patient_nbr','prediction').filter('prediction = 1').show(50)
