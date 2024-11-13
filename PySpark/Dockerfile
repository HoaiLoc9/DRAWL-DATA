FROM openjdk:8-jdk

# Install Spark
ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3

RUN apt-get update && \
    apt-get install -y curl python3-pip && \
    curl -O https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Set working directory
WORKDIR /app

# Copy project files
COPY . /app

# Install Python dependencies
COPY requirements.txt /app/
RUN pip3 install -r /app/requirements.txt

# Copy PostgreSQL JDBC driver
COPY postgresql-42.7.4.jar /app/

# Set CMD to run your PySpark script with required packages
CMD ["spark-submit", "--packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1", "--jars", "/app/postgresql-42.7.4.jar", "/app/PYSPARK.py"]
