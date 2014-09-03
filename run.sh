rm -fR /home/hadoop_data/big_file.parquet


# en modo local, el driver y el executor son el mismo proceso
export SPARK_DRIVER_MEMORY=2G
#  --master local[4] \

# en modo Spark standalone cluster, hay que darle memoria a cada executor
# esta memoria tiene que ser inferior o igual a la memoria asignada al worker

#  --master spark://francois-virtual-machine:7077 \
#  --executor-memory 2G \


# modo local
echo "ejecutando Spark en modo local"

/home/swf/spark-1.0.2-bin-hadoop2/bin/spark-submit \
  --class "cl.fbd.spark.SimpleApp5" \
  --master local[4] \
  target/scala-2.10/simple-spark-project_2.10-1.0.jar

