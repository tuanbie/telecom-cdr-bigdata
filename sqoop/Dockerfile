FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

ENV SQOOP_VERSION=1.4.7
ENV SQOOP_HOME=/opt/sqoop
ENV PATH=$PATH:$SQOOP_HOME/bin
ENV HADOOP_CLASSPATH=$SQOOP_HOME/lib/*

COPY sqoop.tar.gz /tmp/sqoop.tar.gz
COPY mysql-connector-java.jar /tmp/mysql-connector-java.jar
COPY commons-lang-2.6.jar /tmp/commons-lang-2.6.jar

RUN tar -xzf /tmp/sqoop.tar.gz -C /opt && \
    mv /opt/sqoop-1.4.7.bin__hadoop-2.6.0 $SQOOP_HOME && \
    rm /tmp/sqoop.tar.gz && \
    mv /tmp/mysql-connector-java.jar $SQOOP_HOME/lib/mysql-connector-java.jar && \
    mv /tmp/commons-lang-2.6.jar $SQOOP_HOME/lib/commons-lang-2.6.jar

CMD ["tail", "-f", "/dev/null"]
