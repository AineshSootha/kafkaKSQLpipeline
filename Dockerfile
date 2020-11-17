FROM ubuntu:latest



RUN apt-get update && apt-get -y install python3 \
    python3-pip


RUN apt-get install -y libmysqlclient-dev
#RUN apt-get update && apt-get install -y \
#    python-pip
#RUN python3 get-pip.py
RUN python3 -m pip install mysqlclient\
    pip install kafka-python\
    pip install avro-python3\
    pip install "confluent-kafka[avro]"\
    pip install requests
    #pip install mysql-python

#pip install python-schema-registry-client
#RUN mkdir /usr/src/app

#ADD producer.py /
#ADD consumer.py /
ADD confluentProducer.py /
#ADD customers.csv /
#ADD schemaPython.avsc /

COPY ["./confluentProducer.py", "./"]
#COPY ["./consumer.py", "./"]

CMD ["python3", "./confluentProducer.py"]
