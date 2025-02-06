import grpc
import os
import calculator_pb2
import calculator_pb2_grpc
# import psutil
# import time
from kafka import KafkaConsumer
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST, REGISTRY
from prometheus_flask_exporter import PrometheusMetrics
from flask import Flask, request, make_response
from werkzeug.exceptions import BadRequest

app = Flask(__name__)
metrics = PrometheusMetrics(app)
metrics.info('app_info', 'Application info', version='1.0.3')
env_calc = os.environ.get("CALCULATOR_URI")
env_kafka = os.environ.get("KAFKA_URI")

CALCULATOR_URI = env_calc if env_calc else "localhost:50052"
KAFKA_URI = env_kafka if env_kafka else "localhost:9092"

# Custom metrics
# REQUEST_COUNT = Counter('http_request_total', 'Total HTTP Requests', ['method', 'status', 'path'])
# REQUEST_LATENCY = Histogram('http_request_duration_seconds', 'HTTP Request Duration', ['method', 'status', 'path'])
# REQUEST_IN_PROGRESS = Gauge('http_requests_in_progress', 'HTTP Requests in progress', ['method', 'path'])

# # System metrics
# CPU_USAGE = Gauge('process_cpu_usage', 'Current CPU usage in percent')
# MEMORY_USAGE = Gauge('process_memory_usage_bytes', 'Current memory usage in bytes')

# def update_system_metrics():
#     CPU_USAGE.set(psutil.cpu_percent())
#     MEMORY_USAGE.set(psutil.Process().memory_info().rss)

# @app.before_request
# def log_request_info():
#     request.start_time = time.time()
#     print(f"Request: {request.method} {request.url}")

# @app.after_request
# def log_response_info(response):
#     duration = time.time() - request.start_time
#     method = request.method
#     path = request.url
#     status = response.status_code
#     REQUEST_IN_PROGRESS.labels(method=method, path=path).inc()
#     REQUEST_COUNT.labels(method=method, status=status, path=path).inc()
#     REQUEST_LATENCY.labels(method=method, status=status, path=path).observe(duration)
#     REQUEST_IN_PROGRESS.labels(method=method, path=path).dec()
#     return response


@app.get("/")
@metrics.histogram('http_request_duration_seconds', 'Request latencies by status and path',
                   labels={'status': lambda r: r.status_code, 'path': lambda: request.path})
@metrics.gauge('process_cpu_usage', 'Current CPU usage in percent')
@metrics.gauge('process_memory_usage_bytes', 'Current memory usage in bytes')
def index():
    with grpc.insecure_channel(CALCULATOR_URI) as channel:
        stub = calculator_pb2_grpc.CalculatorStub(channel)
        response = stub.Add(calculator_pb2.CalculationRequest(number1=1, number2=2))
        return make_response(
            {"message": "Basic demonstration control API", "'1+2'": response.result}
        )
    
@app.get("/kafka")
@metrics.do_not_track()
def get_kafka():
    consumer = KafkaConsumer("words", bootstrap_servers=KAFKA_URI)
    words = next(consumer).value.decode("utf-8")
    return make_response({"words": words})

@app.post("/")
@metrics.do_not_track()
def post_message():
    try:
        message = request.json["message"]
        message = request
        # Pass message onward here
        return make_response({"message": f"processed message '{message}'"})
    except KeyError:
        raise BadRequest()

# @app.get("/metrics")
# def metrics():
#   update_system_metrics()
#   return make_response({"metrics": f"message '{generate_latest(REGISTRY)}'"})
@app.errorhandler(BadRequest)
@metrics.do_not_track()
def handle_bad_request(e):
    return make_response(e.description, 400)
