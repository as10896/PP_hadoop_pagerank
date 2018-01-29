#!/bin/bash

INPUT_FILE=[your input path]
OUTPUT_FILE=PageRank/Output
JAR=PageRank.jar
# ITER=10

hdfs dfs -rm -r PageRank
# hadoop jar $JAR pageRank.PageRank $INPUT_FILE $OUTPUT_FILE $ITER
hadoop jar $JAR pageRank.PageRank $INPUT_FILE $OUTPUT_FILE
hdfs dfs -getmerge $OUTPUT_FILE output.txt
