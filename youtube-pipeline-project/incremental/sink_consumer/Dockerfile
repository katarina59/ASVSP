FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y netcat-openbsd && apt-get clean

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .
COPY wait-for-kafka.sh .
RUN chmod +x wait-for-kafka.sh

CMD ["./wait-for-kafka.sh", "kafka", "9092", "python", "consumer.py"]
