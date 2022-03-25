FROM ubuntu:focal

ENV FLASK_APP=erddap_handler \
	TZ="Etc/UTC" \
	DEBIAN_FRONTEND="noninteractive" \
	WIS2BOX_DATA="/local/data/"

WORKDIR /tmp/erddap

RUN apt-get update -y \
    && apt-get install -y python3 python3-pip python3-dev

COPY . /tmp/erddap_plugin

RUN cd /tmp/erddap_plugin && python3 setup.py install

EXPOSE 5000

CMD ["flask","run","--host=0.0.0.0"]