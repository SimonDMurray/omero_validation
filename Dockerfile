ARG ubuntu_version=18.04

FROM ubuntu:$ubuntu_version

ENV DEBIAN_FRONTEND=noninteractive 

RUN apt-get update && apt-get install -y \
    cmake bzip2 build-essential software-properties-common \
    libssl-dev libbz2-dev

RUN set -xe \
    && apt-get -y update \
    && apt-get -y install python3.6 \
    && apt-get -y install python3-dev \
    && apt-get -y install python3-pip 

RUN pip3 install --upgrade pip

RUN pip3 install --upgrade --no-cache \
    openpyxl pandas omero-py
