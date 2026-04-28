FROM python:3.11-alpine

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/car_exporter.py .
COPY dataset/global_cars_enhanced.csv .

EXPOSE 8000

CMD ["python", "car_exporter.py"]