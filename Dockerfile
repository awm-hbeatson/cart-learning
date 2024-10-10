FROM python:3.10-slim

RUN apt-get update && apt-get upgrade -y python3-pip python3-distutils && apt-get install librdkafka-dev -y

# Install python packages
# RUN pip3 install --upgrade pip && \
#     pip3 install numpy orjson confluent-kafka opencv-contrib-python requests requests_toolbelt \
#     boto3 opencv-python pytz pymysql jaeger-client pymysql cassandra_driver redis boto3 cryptography 
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

RUN pip3 install opentelemetry-distro opentelemetry-exporter-otlp opentelemetry-instrumentation-logging
RUN opentelemetry-bootstrap -a install


# Copy scripts folder into image
COPY src/ src/

#Change workdir
WORKDIR src/
#CMD flask --app aggregator_api.py --debug run
#CMD python3 main.py
CMD python3 main.py
#CMD sleep 10000