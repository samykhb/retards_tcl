cd 2_processing
docker compose -f docker-compose.yml up -d
docker exec spark-history-tcl bash /opt/spark/sbin/start-history-server.sh # Start the Spark History Server inside the spark-history-tcl container Job finishes → still visible at localhost:18080