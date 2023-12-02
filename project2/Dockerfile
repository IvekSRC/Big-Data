FROM bde2020/spark-python-template:3.1.2-hadoop3.2
RUN cd /app pip install -r requirements.txt
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/main.py
ENV SPARK_APPLICATION_ARGS /app/.env
ENV SPARK_SUBMIT_ARGS "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2"