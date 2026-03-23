Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
docker exec spark-master-tcl rm -rf /app/checkpoint/delay_stats;
docker exec -it spark-master-tcl spark-submit --master spark://spark-master-tcl:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,com.datastax.spark:spark-cassandra-connector_2.13:3.5.0 --conf spark.jars.ivy=/opt/spark-apps/.ivy2 /opt/spark-apps/speed_jobs/delay_detection.py
