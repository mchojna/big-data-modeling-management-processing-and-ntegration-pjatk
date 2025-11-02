FROM eclipse-temurin:11-jre-jammy

ARG SPARK_VERSION=3.5.1
ARG HADOOP_PROFILE=hadoop3
ARG HADOOP_AWS_VERSION=3.3.6
ARG AWS_SDK_BUNDLE_VERSION=1.12.262

RUN apt-get update && apt-get install -y --no-install-recommends \
python3 python3-pip curl ca-certificates tini bash netcat-openbsd \
&& rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz \
    -o /tmp/spark.tgz \
    && mkdir -p ${SPARK_HOME} \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE} ${SPARK_HOME} \
    && rm -f /tmp/spark.tgz

# biblioteki s3a
RUN mkdir -p ${SPARK_HOME}/jars \
    && curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    -o ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    && curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar \
    -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar

WORKDIR /opt/app
COPY requirements.txt /opt/app/requirements.txt

RUN pip3 install --no-cache-dir -r requirements.txt
COPY app /app

ENTRYPOINT ["/usr/bin/tini","--"]
CMD ["bash"]