mkdir Downloads

#go to downloads folder
cd Downloads

# download the dataset
wget http://files.grouplens.org/datasets/movielens/ml-latest.zip 

# unzip
unzip ml-latest.zip

# create hdfs folders
hdfs dfs -mkdir -p /user/raj_ops/rawdata/movielens/latest/movies
hdfs dfs -mkdir -p /user/raj_ops/rawdata/movielens/latest/movies_parquet

# copy movie dataset to hdfs
hdfs dfs -moveFromLocal ml-latest/movies.csv /user/raj_ops/rawdata/movielens/latest/movies