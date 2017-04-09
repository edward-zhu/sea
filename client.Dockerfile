FROM phusion/baseimage:0.9.19

ADD ./requirements.txt ./gcs-service-account.json /app/

ENV PYTHONPATH="/app" \
    GOOGLE_APPLICATION_CREDENTIALS="/app/gcs-service-account.json" \
    JOB_TRACKER="http://sea-master:9000"

RUN echo "deb http://packages.cloud.google.com/apt gcsfuse-xenial main" | tee /etc/apt/sources.list.d/gcsfuse.list; \ 
  curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \ 
  && apt-get update \
  && apt-get install -y apt-utils kmod fuse gcsfuse python3-pip python3-setuptools --no-install-recommends \
  && mkdir -p /app/data/gcloud \
  && ln -s /usr/bin/python3 /usr/local/bin/python \
  && ln -s /usr/bin/pip3 /usr/local/bin/pip \
  && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN pip install wheel \
    && pip install -r /app/requirements.txt \
    && mkdir /etc/service/servalclient

ADD . /app
COPY ./image_file/client.rc.local /etc/rc.local
COPY ./image_file/start_task_tracker.sh /etc/service/servalclient/run
RUN chmod +x /etc/service/servalclient/run \
    && chmod +x /etc/rc.local

EXPOSE 8800

CMD ["/sbin/my_init"]
