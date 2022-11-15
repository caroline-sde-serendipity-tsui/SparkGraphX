#!/bin/sh

# Run in terminal:
# ./run.sh MST <dataset name>
# ./run.sh MST datasettest

spark-submit --class $1 --master local $1.jar $2 $3 $4
