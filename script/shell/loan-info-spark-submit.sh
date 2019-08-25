#!/bin/sh
env=$1
batchDuration=$2
echo "当前环境>>>>>>$env"

spark2-submit \
  --class weshare.data.center.taskinit.LoanInfoStreamInit \
  --master yarn \
  --deploy-mode client \
  --driver-memory 2G \
  --num-executors 2 \
  --executor-memory 1G \
  --executor-cores 1 \
  --conf "spark.driver.extraJavaOptions=-Dscala.env=$env" \
  --conf "spark.executor.extraJavaOptions=-Dscala.env=$env" \
  data-application-0.0.1.jar $batchDuration
