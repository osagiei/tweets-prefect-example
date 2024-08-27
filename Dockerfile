#FROM openjdk:8-jdk
FROM openjdk:8-jre-slim

RUN apt-get update && apt-get install -y python3 python3-pip python3-dev bash gcc g++ curl
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN pip3 install --upgrade pip

RUN pip3 install pyspark==3.2.0 prefect==2.20.3

ENV JAVA_HOME=/usr/local/openjdk-8
ENV PATH=$JAVA_HOME/bin:$PATH

RUN java -version

COPY . /app
WORKDIR /app

CMD ["python", "prefect_flow.py"]

