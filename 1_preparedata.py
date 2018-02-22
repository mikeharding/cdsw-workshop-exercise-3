#Gives schema to data
#Transforms a new column isreadmitted as 1 or 0
#Builds a parquet table


from pyspark.sql import SparkSession
from pyspark.sql.functions import split, from_unixtime
from pyspark.sql.types import *
from pyspark.sql.functions import when


# Boilerplate Spark stuff:
spark = SparkSession\
    .builder\
    .appName("diabetes_patient_readmission_datapreparation")\
    .getOrCreate()



rawDataFrame = spark.read.\
               option("header","true").\
               option("inferSchema","true").\
               csv("hdfs:///tmp/diabetes/diabetic_data_original.csv")



rawDataFrame.show()

#Pick data that we care about, ignoring fields with only ? : example: payer_code
rawDataFrameFiltered = rawDataFrame.selectExpr("`encounter_id` as encounter_id",
              "`patient_nbr` as patient_nbr",
              "`race` as race",
              "`gender` as gender",
              "`age` as age",
              "`admission_type_id` as admission_type_id",
              "`discharge_disposition_id` as discharge_disposition_id",
               "`admission_source_id` as admission_source_id",
              "`time_in_hospital` as time_in_hospital",
              "`num_lab_procedures` as num_lab_procedures",
              "`num_procedures` as num_procedures",
              "`num_medications` as num_medications",
              "`number_outpatient` as number_outpatient",
              "`number_inpatient` as number_inpatient",
              "`number_diagnoses` as number_diagnoses",
              "`max_glu_serum`  as  max_glu_serum",
             "`A1Cresult`  as   A1Cresult",
             "`metformin`  as   metformin",
             "`repaglinide`  as repaglinide",
            "`nateglinide`  as  nateglinide",
            "`chlorpropamide`  as   chlorpropamide",
            "`glimepiride`  as  glimepiride",
            "`acetohexamide`  as    acetohexamide",
            "`glipizide`  as    glipizide",
            "`glyburide`  as    glyburide",
            "`tolbutamide`  as  tolbutamide",
            "`pioglitazone`  as pioglitazone",
            "`rosiglitazone`  as    rosiglitazone",
            "`acarbose`  as acarbose",
            "`miglitol`  as miglitol",
            "`troglitazone`  as troglitazone",
            "`tolazamide`  as   tolazamide",
            "`examide`  as  examide",
            "`citoglipton`  as  citoglipton",
            "`insulin`  as  insulin",
            "`glyburide-metformin`  as  glyburide_metformin",
            "`glipizide-metformin`  as  glipizide_metformin",
            "`glimepiride-pioglitazone`  as glimepiride_pioglitazone",
            "`metformin-rosiglitazone`  as  metformin_rosiglitazon",
            "`metformin-pioglitazone`  as   metformin_pioglitazone",
            "`change`  as   change",
            "`diabetesMed`  as  diabetesMed",
            "`readmitted`  as   readmitted"
            )


rawDataFrameFiltered= rawDataFrameFiltered.withColumn("isreadmitted",when(rawDataFrameFiltered.readmitted=='<30', 1).otherwise(0))

# Show top rows
rawDataFrameFiltered.select('readmitted','isreadmitted').show(50)
rawDataFrameFiltered.printSchema()

rawDataFrameFiltered.write.saveAsTable('diabetic_data_original_parquet', format="parquet", mode="overwrite")

spark.stop()
