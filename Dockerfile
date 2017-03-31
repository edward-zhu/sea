FROM python:3.6

ADD . /app

RUN pip install -r /app/requirements.txt

ENV PYTHONPATH="/app"

CMD ["python", "/app/client/reformatter.py"]
