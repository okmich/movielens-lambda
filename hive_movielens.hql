create database movielens_lambda;

use movielens_lambda;
-- create the raw table for the data downloaded
create external table movie_raw(
	movieId bigint,
	title string,
	genre string
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
	"separatorChar" = ",",
	"quoteChar"     = "\"",
	"escapeChar"    = "\\"
)
stored as textfile
location '/user/raj_ops/rawdata/movielens/latest/movies';

-- create another table movie for the movie dataset
-- in parquet format, remove the header from the source
create external table movie(
	movieId bigint,
	title string,
	genre array<string>
)
stored as parquet
location '/user/raj_ops/rawdata/movielens/latest/movies_parquet';

-- copy from raw to parquet
-- remove headers in the process
insert overwrite table movie
select movieId, title, split(genre, "\\|") from movie_raw where title <> 'title';

-- confirm that all data is written as expected
select count(1) from movie;