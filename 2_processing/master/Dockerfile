FROM spark:4.0.1-java21-python3 AS builder

USER root
RUN apt-get update;
RUN pip install --upgrade pip  --no-cache-dir
RUN pip install pandas --no-cache-dir
RUN pip install pyspark==4.0.1 --no-cache-dir
RUN pip install lz4==4.3.3   --no-cache-dir
RUN pip install jupyterlab   --no-cache-dir
RUN pip install jupyter   --no-cache-dir
RUN pip install notebook   --no-cache-dir
RUN pip install findspark   --no-cache-dir
RUN pip install pyarrow   --no-cache-dir

RUN pip install scales  --no-cache-dir

RUN apt-get update; apt install -y netcat
RUN apt install -y rsync


# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater
ENV SPARK_VERSION=4.0.1 \
HADOOP_VERSION=4 \
SPARK_HOME=/opt/spark \
PYTHONHASHSEED=1

FROM builder AS apache-sparktmp

WORKDIR /opt/spark-apps

ENV SPARK_MASTER_PORT=7077 \
SPARK_MASTER_WEBUI_PORT=8080 \
SPARK_LOG_DIR=/opt/spark/logs \
SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
SPARK_WORKER_WEBUI_PORT=8080 \
SPARK_WORKER_PORT=7000 \
SPARK_MASTER="spark://spark-master-tcl:7077" \
SPARK_WORKLOAD="master"

EXPOSE 8080 7077 7000 4040 8888

RUN mkdir -p /opt/spark/conf
RUN mkdir -p /tmp/spark-events
RUN mkdir -p $SPARK_LOG_DIR

RUN mkdir -p $SPARK_LOG_DIR && \
touch $SPARK_MASTER_LOG && \
touch $SPARK_WORKER_LOG && \
ln -sf /dev/stdout $SPARK_MASTER_LOG && \
ln -sf /dev/stdout $SPARK_WORKER_LOG

COPY spark-defaults.conf "${SPARK_HOME}/conf"
COPY start-spark.sh /

CMD ["/bin/bash", "/start-spark.sh"]