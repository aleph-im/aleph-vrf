FROM --platform=linux/amd64 debian:bookworm

RUN apt-get update && apt-get -y upgrade && apt-get install -y \
     git \
     libsecp256k1-dev \
     python3-pip \
     python3-venv \
     squashfs-tools \
     && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/aleph_vrf
COPY . .

RUN mkdir /opt/packages
RUN pip install -t /opt/packages .

ENV PYTHONPATH=/opt/packages
