FROM python:3.6

RUN export GCSFUSE_REPO=gcsfuse-jessie \
	&& echo "deb http://packages.cloud.google.com/apt $GCSFUSE_REPO main" | tee /etc/apt/sources.list.d/gcsfuse.list \
	&& curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
	&& apt-get update && apt-get install gcsfuse ntpdate -y

ADD requirements.txt /app/

RUN pip install -r /app/requirements.txt

ADD start_host.sh /app/
	
RUN chmod +x /app/start_host.sh

ENV ETCD_CLUSTER="$HOSTIP:2379" \
	DATA_BASE="/gcloud/test/"

ADD . /app

CMD /app/start_host.sh