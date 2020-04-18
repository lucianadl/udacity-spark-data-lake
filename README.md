# Udacity - Data Engineering Nanodegree
## Project: Data Lake

### Summary

The startup Sparkify has a music streaming app, and its data related to songs, artists and listening behavior resides in JSON files stored in Amazon S3. 

There are two main types of files:
* JSON logs on user activity on the app (`s3://udacity-dend/log_data`)
* JSON metadata on the songs available in the app (`s3://udacity-dend/song_data`)

The purpose of this project is building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow the analytics team of the company to continue finding insights in what songs their users are listening to.

The dimension tables defined for the schema are the attributes that the analytics team is interested to understand in user activity: the actual *users*, the *songs* listened to by the users, the *artists* of those songs, and the *time* identifying when the songs started to be listened to. The fact table was defined to be the actual *songplays* registered by the app.

### Configuration file

The configuration parameters of this project are defined in the file `dl.cfg`. 

Before running any script, ensure that the AWS credentials (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY) are correctly set in the configuration file.

Also, set the OUTPUT_DATA parameter with the path for the S3 bucket where the Data Lake will be hosted.

```
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=

[S3]
INPUT_DATA=s3a://udacity-dend/
OUTPUT_DATA=
```

### Processing the data

The ETL pipeline for each table were developed in a Python script. The data is loaded from the S3 bucket defined in the INPUT_DATA parameter of the configuration file. The data is processed using Spark, and loaded back into the S3 bucket defined in the OUTPUT_DATA parameter.

To process the ETL pipeline, run the following command in the terminal:

```
python etl.py
```

Each of the five tables will be written to parquet files in a separate analytics directory on S3:

 * `songs.parquet` (partitioned by year and then artist)
 * `artists.parquet`
 * `users.parquet`
 * `time.parquet` (partitioned by year and month)
 * `songplays.parquet` (partitioned by year and month)
 