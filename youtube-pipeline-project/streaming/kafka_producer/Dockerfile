FROM python:3.10-slim

WORKDIR /app

RUN pip install kafka-python==3.0.9 requests

COPY producer.py .

CMD ["python", "producer.py"]