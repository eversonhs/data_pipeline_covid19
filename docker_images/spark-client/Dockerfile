FROM apache/spark-py
COPY bin/connectorj/mysql-connector-j_8.1.0-1ubuntu22.04_all.deb /opt/spark/work-dir

USER root
RUN apt install /opt/spark/work-dir/mysql-connector-j_8.1.0-1ubuntu22.04_all.deb
ENV MYSQL_CONNECTORJ_PATH=/usr/share/java/mysql-connector-j-8.1.0.jar
ENV PATH="${PATH}:/opt/spark/bin"