from http.server import HTTPServer, BaseHTTPRequestHandler
from prometheus_client import Gauge, Counter
import random
import time
import threading


car_price_gauge = Gauge(
    'car_market_price_usd',
    'Average car price by brand',
    ['brand', 'body_type']
)

car_count_gauge = Gauge(
    'car_market_count',
    'Number of cars by category',
    ['brand', 'fuel_type']
)

car_efficiency_gauge = Gauge(
    'car_market_efficiency_score',
    'Efficiency score of cars',
    ['brand']
)

total_requests = Counter(
    'car_exporter_requests_total',
    'Total requests to exporter'
)


class CarMarketExporter:
    def __init__(self):
        self.brands = ['Toyota', 'BMW', 'Mercedes', 'Audi', 'Nissan', 'Tesla']
        self.body_types = ['SUV', 'Sedan', 'Coupe', 'Hatchback']
        self.fuel_types = ['Petrol', 'Diesel', 'Electric', 'Hybrid']
        
    def get_metrics(self):
        """Генерация метрик для Prometheus"""
        metrics = []
        current_time = int(time.time() * 1000)
        
        for brand in self.brands:
            for body_type in self.body_types:
                price = random.uniform(20000, 120000)
                labels = f'{{brand="{brand}", body_type="{body_type}"}}'
                metrics.append(f'car_market_price_usd{labels} {price:.2f} {current_time}')
            
            for fuel_type in self.fuel_types:
                count = random.randint(10, 100)
                labels = f'{{brand="{brand}", fuel_type="{fuel_type}"}}'
                metrics.append(f'car_market_count{labels} {count} {current_time}')
            
            efficiency = random.uniform(0.1, 1.0)
            labels = f'{{brand="{brand}"}}'
            metrics.append(f'car_market_efficiency_score{labels} {efficiency:.4f} {current_time}')
        
        return "\n".join(metrics)


class ExporterHTTPHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        total_requests.inc()
        
        if self.path == '/metrics':
            exporter = CarMarketExporter()
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; version=0.0.4')
            self.end_headers()
            self.wfile.write(exporter.get_metrics().encode())
        elif self.path == '/health':
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'OK')
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'Not Found')
    
    def log_message(self, format, *args):
        pass


def run_server():
    server = HTTPServer(('0.0.0.0', 8000), ExporterHTTPHandler)
    print("Car Market Exporter running on http://0.0.0.0:8000/metrics")
    server.serve_forever()


if __name__ == '__main__':
    run_server()