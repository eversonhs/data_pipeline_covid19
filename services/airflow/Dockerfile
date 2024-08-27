FROM apache/airflow:2.10.0
COPY bin/postgresql/postgresql-42.7.4.jar /usr/share/java/
ENV POSTGRESQL_JDBC_DRIVER=/usr/share/java/postgresql-42.7.4.jar
COPY requirements.txt /home/
USER root
RUN apt-get update -y
RUN apt-get install -y openjdk-17-jdk
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
USER airflow
RUN pip install -r /home/requirements.txt
