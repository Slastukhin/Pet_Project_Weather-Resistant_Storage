# syntax=docker/dockerfile:1
FROM spark:3.5.8-scala2.12-java17-python3-ubuntu

ADD --chmod=644 https://jdbc.postgresql.org/download/postgresql-42.7.8.jar /opt/spark/jars/postgresql-42.7.8.jar

USER spark
WORKDIR /opt/spark/jobs
