FROM ubuntu:latest

ADD producer.py /
ADD consumer.py /
ADD customers.csv /

RUN apt-get update && apt-get -y install python3 \
    python3-pip


RUN apt-get install -y libmysqlclient-dev
#RUN apt-get update && apt-get install -y \
#    python-pip
#RUN python3 get-pip.py
RUN python3 -m pip install mysqlclient\
    pip install kafka-python\
    pip install avro-python3\
    pip install confluent-kafka\
    pip install requests
#RUN mkdir /usr/src/app

COPY ["./producer.py", "./"]
COPY ["./consumer.py", "./"]

CMD ["python3", "./producer.py"]
