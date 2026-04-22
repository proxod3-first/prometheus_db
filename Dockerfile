# Используем официальный образ Python
FROM python:3.11-alpine

WORKDIR /app

COPY app/car_exporter.py .

RUN pip install --no-cache-dir prometheus-client

EXPOSE 8000

CMD ["python", "car_exporter.py"]