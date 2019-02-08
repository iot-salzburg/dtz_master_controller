FROM python:3.6
# not using onbuild, because changed code results in installation time

MAINTAINER Armin Niedermueller <armin.niedermueller@salzburgresearch.at>

# test internet connection and dns settings. If apt-get update fails, restart
# docker service, check internet connection and dns settings in /etc/docker/daemon.json
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libffi-dev \
    libxml2-dev \
    libxslt-dev \
#    python-dev \
    python3-lxml \
#    python-lxml \
    libssl-dev \
    gcc && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy the content of this folder into the hosts home directory.
ADD requirements.txt .
RUN pip3 install -r requirements.txt

ADD . .

# setup proper configuration
ENV PYTHONPATH .

ENTRYPOINT ["python", "opc_ua_master.py"]
