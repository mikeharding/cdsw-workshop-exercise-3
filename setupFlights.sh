#############################################################
#
# Download dataset from Stat Computing and copy to HDFS
#
#############################################################

## download weather data file from ncdc
cd data
rm *.bz2

## download weather files for 1987-2008 in tar format
for i in $(seq 2005 2008)
do
  ## download weather file for each year
  wget http://stat-computing.org/dataexpo/2009/$i.csv.bz2

  ## untar and unzip them to *.op file
  bzip2 -d *.bz2

done

## copy weather data from local container to hdfs weather folder
##hdfs dfs -mkdir flights
hdfs dfs -copyFromLocal *.csv flights
echo "flight files copied to HDFS."
