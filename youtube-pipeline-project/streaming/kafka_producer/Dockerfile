FROM python:3.10-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir kafka-python requests

CMD ["python", "producer.py"]
