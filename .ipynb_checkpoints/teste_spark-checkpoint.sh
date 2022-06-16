spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master yarn \
  --deploy-mode cluster \
  --supervise \
  --conf "spark.pyspark.driver.python=/usr/bin/python3"\
  --conf "spark.hadoop.metastore.catalog.default=hive"\
  --conf "spark.pyspark.python=/usr/bin/python3"\
  --conf 'spark.executorEnv.PYTHONPATH=deps' \
  --executor-memory 2G \
  --total-executor-cores 100 \
  app.py \
  1000






#/usr/local/spark/examples/src/main/python/pi.py
#putTweet.py
#getTweet.py
#getTweetFromHDFS.py \

