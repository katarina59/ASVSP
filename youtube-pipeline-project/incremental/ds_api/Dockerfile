FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir fastapi uvicorn[standard]

COPY app.py .
COPY data ./data

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
