FROM python:3.6

ADD . /app/requirements.txt

RUN pip install -r /app/requirements.txt

ADD . /app

ENV PYTHONPATH="/app"

CMD ["python", "/app/client/reformatter.py"]
