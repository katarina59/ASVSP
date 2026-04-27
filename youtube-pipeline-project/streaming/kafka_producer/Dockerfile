FROM python:3.10-slim

WORKDIR /app

RUN pip install kafka-python requests

COPY producer.py .

CMD ["python", "producer.py"]