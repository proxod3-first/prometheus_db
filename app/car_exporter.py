from http.server import HTTPServer, BaseHTTPRequestHandler
from prometheus_client import Gauge, Counter, Info
from urllib.request import urlopen, Request
import json
import time
import logging
import csv
import os
import random
import hashlib


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


car_price_usd_gauge = Gauge(
    'car_price_usd',
    'Car price in USD by brand and model',
    ['car_id', 'brand', 'model_year']
)

car_price_eur_gauge = Gauge(
    'car_price_eur',
    'Car price in EUR by brand and model',
    ['car_id', 'brand', 'model_year']
)

car_price_rub_gauge = Gauge(
    'car_price_rub',
    'Car price in RUB by brand and model',
    ['car_id', 'brand', 'model_year']
)

exchange_rate_gauge = Gauge(
    'exchange_rate_to_usd',
    'Exchange rate to USD by currency code',
    ['currency']
)

exporter_requests_total = Counter(
    'car_exporter_requests_total',
    'Total requests to car exporter',
    ['endpoint']
)

exporter_errors_total = Counter(
    'car_exporter_errors_total',
    'Total errors during data fetching',
    ['error_type']
)

exporter_last_update = Gauge(
    'car_exporter_last_update_timestamp',
    'Timestamp of last successful data update'
)

exporter_api_fetch_count = Counter(
    'car_exporter_api_fetch_count',
    'Number of times data was fetched from external API'
)

exporter_info = Info(
    'car_currency_exporter',
    'Information about car currency exporter'
)
exporter_info.info({
    'version': '1.0.0',
    'author': 'Me',
    'source': 'ExchangeRate-API',
    'source_url': 'https://www.exchangerate-api.com/',
    'update_interval': '3 seconds'
})


CSV_FILE_PATH = os.path.join(os.path.dirname(__file__), 'global_cars_enhanced.csv')


API_KEY = os.getenv('EXCHANGE_RATE_API_KEY', 'YOUR_API_KEY_HERE')
EXCHANGE_API_URL = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD"


# Интервалы обновления:
API_CACHE_TTL = 86400    # 24 часа
METRIC_CACHE_TTL = 3     # 3 сек

data_cache = {
    'base_rates': None,
    'cars': None,
    'last_api_update': 0,
    'last_metric_update': 0,
    'simulation_seed': None
}

TARGET_CURRENCIES = ['EUR', 'RUB', 'CNY', 'GBP', 'JPY']

# Параметры симуляции
SIMULATION_VOLATILITY = 0.02
FALLBACK_RATES = {
    'EUR': 0.92, 'RUB': 92.5, 'CNY': 7.2, 'GBP': 0.79, 'JPY': 150.0
}


def fetch_exchange_rates():
    """Получение реальных курсов валют из ExchangeRate-API"""

    try:
        req = Request(EXCHANGE_API_URL)
        req.add_header('User-Agent', 'Prometheus-Exporter/1.0')
        
        with urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode('utf-8'))
            
            if data.get('result') == 'success':
                rates = data.get('conversion_rates', {})
                filtered = {
                    curr: rates.get(curr, FALLBACK_RATES.get(curr, 0)) 
                    for curr in TARGET_CURRENCIES
                }
                logger.info(f"Получено курсов из API: {len(filtered)}")
                exporter_api_fetch_count.inc()
                return filtered
            else:
                logger.warning(f"API error: {data.get('error', 'Unknown')}")
                return FALLBACK_RATES.copy()
                
    except Exception as e:
        logger.warning(f"Ошибка API, используем fallback: {e}")
        exporter_errors_total.labels(error_type='exchange_api').inc()
        return FALLBACK_RATES.copy()


def load_car_data():
    """Загрузка данных об автомобилях из CSV"""

    cars = []
    try:
        with open(CSV_FILE_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    price = float(row['Price_USD']) if row['Price_USD'] else 0
                    if price > 0:
                        cars.append({
                            'car_id': row['Car_ID'],
                            'brand': row['Brand'],
                            'year': row['Manufacture_Year'],
                            'price_usd': price
                        })
                except (ValueError, KeyError):
                    continue
        logger.info(f"Загружено {len(cars)} записей из {CSV_FILE_PATH}")
        return cars
    except Exception as e:
        logger.error(f"Ошибка загрузки CSV: {e}")
        exporter_errors_total.labels(error_type='csv_load').inc()
        return []


def get_simulation_factor(currency, time_window):
    """
    Генерация воспроизводимого коэффициента симуляции
    """
    
    seed_str = f"{currency}_{time_window}"
    seed = int(hashlib.md5(seed_str.encode()).hexdigest()[:8], 16)
    random.seed(seed)
    factor = 1 + random.uniform(-SIMULATION_VOLATILITY, SIMULATION_VOLATILITY)
    return round(factor, 4)


def update_cache():
    current_time = time.time()
    
    if current_time - data_cache['last_api_update'] >= API_CACHE_TTL:
        logger.info("Обновление базовых курсов из API...")
        data_cache['base_rates'] = fetch_exchange_rates()
        data_cache['last_api_update'] = current_time

    if data_cache['cars'] is None:
        data_cache['cars'] = load_car_data()

    if current_time - data_cache['last_metric_update'] >= METRIC_CACHE_TTL:
        time_window = int(current_time // METRIC_CACHE_TTL)
        data_cache['simulation_seed'] = time_window
        data_cache['last_metric_update'] = current_time
        logger.info(f"Метрики обновлены (окно: {time_window})")


def generate_metrics():
    update_cache()
    metrics = []
    current_time = int(time.time() * 1000)
    
    base_rates = data_cache.get('base_rates', FALLBACK_RATES)
    time_window = data_cache.get('simulation_seed', int(time.time() // METRIC_CACHE_TTL))
    cars = data_cache.get('cars', [])[:30] # Первые 30 для демо
    
    for currency in TARGET_CURRENCIES:
        base_rate = base_rates.get(currency, FALLBACK_RATES.get(currency, 0))
        if base_rate > 0:
            factor = get_simulation_factor(currency, time_window)
            simulated_rate = round(base_rate * factor, 4)
            
            labels = f'{{currency="{currency}"}}'
            metrics.append(f'exchange_rate_to_usd{labels} {simulated_rate} {current_time}')
    
    for car in cars:
        car_id = car['car_id']
        brand = car['brand']
        year = car['year']
        price_usd = car['price_usd']
        
        labels = f'{{car_id="{car_id}", brand="{brand}", model_year="{year}"}}'
        
        metrics.append(f'car_price_usd{labels} {price_usd} {current_time}')
        
        base_eur = base_rates.get('EUR', FALLBACK_RATES['EUR'])
        eur_factor = get_simulation_factor('EUR', time_window)
        price_eur = round(price_usd * base_eur * eur_factor, 2)
        metrics.append(f'car_price_eur{labels} {price_eur} {current_time}')
        
        base_rub = base_rates.get('RUB', FALLBACK_RATES['RUB'])
        rub_factor = get_simulation_factor('RUB', time_window)
        price_rub = round(price_usd * base_rub * rub_factor, 2)
        metrics.append(f'car_price_rub{labels} {price_rub} {current_time}')
    
    return "\n".join(metrics)


class ExporterHTTPHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        endpoint = self.path.split('?')[0]
        exporter_requests_total.labels(endpoint=endpoint).inc()
        
        if endpoint == '/metrics':
            try:
                metrics = generate_metrics()
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain; version=0.0.4')
                self.end_headers()
                self.wfile.write(metrics.encode())
                
            except Exception as e:
                logger.error(f"Ошибка генерации метрик: {e}")
                exporter_errors_total.labels(error_type='generation').inc()
                self.send_response(500)
                self.end_headers()
                self.wfile.write(b'Internal Server Error')
                
        elif endpoint == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
            
        elif endpoint == '/info':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            info = {
                'status': 'running',
                'source': 'ExchangeRate-API',
                'api_cache_age': time.time() - data_cache['last_api_update'],
                'metric_cache_age': time.time() - data_cache['last_metric_update'],
                'simulation_window': data_cache.get('simulation_seed'),
                'volatility': SIMULATION_VOLATILITY
            }
            self.wfile.write(f'{json.dumps(info)}\n'.encode())
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'Not Found')
    
    def log_message(self, format, *args):
        pass


def run_server(host='0.0.0.0', port=8000):
    server = HTTPServer((host, port), ExporterHTTPHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Экспортер остановлен")
        server.shutdown()


if __name__ == '__main__':
    if API_KEY:
        logger.warning("API-ключ не установлен! Метрики будут использовать fallback-значения")
    run_server()
