#!/bin/bash

set -ex

hdfs dfs -mkdir /tmp/diabetes
hdfs dfs -put -f data/diabetic_data_original.csv /tmp/diabetes/diabetic_data_original.csv
hdfs dfs -mkdir /tmp/diabetes/diabetic_data_model/
