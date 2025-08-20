FROM python:3.10-slim

# Install netcat for wait script
RUN apt-get update && apt-get install -y netcat-traditional && rm -rf /var/lib/apt/lists/*

# Radni direktorijum
WORKDIR /app

# Instalacija dependencija
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Kopiramo consumer kod i wait script
COPY consumer.py .
COPY wait-for-kafka.sh .
RUN chmod +x wait-for-kafka.sh

# Pokretanje sa wait scriptom
CMD ["./wait-for-kafka.sh", "kafka", "9092", "python", "consumer.py"]