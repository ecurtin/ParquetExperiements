#!/usr/bin/env bash


#directoryRoot="/Users/ecurtin/git/ParquetExperiements/"
#
#echo $directoryRoot
#
#alias parquet-tools="hadoop jar /opt/parquet-tools-1.9.0.jar"
#
#touch $directoryRoot/output/parquet-dumps-as-text/CC-Sample.txt

hadoop jar /opt/parquet-tools-1.9.0.jar dump /Users/ecurtin/git/ParquetExperiements/output/raw-parquet/CC-Sample.parquet >> /Users/ecurtin/git/ParquetExperiements/output/parquet-dumps-as-text/CC-Sample.txt

