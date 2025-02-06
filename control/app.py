import grpc
import os
import calculator_pb2
import calculator_pb2_grpc
import requests
from flask import Flask, request, make_response
from werkzeug.exceptions import BadRequest
from kafka import KafkaConsumer

app = Flask(__name__)
env_calc = os.environ.get("CALCULATOR_URI")
env_kafka = os.environ.get("KAFKA_URI")
enc_kafkaservice = os.environ.get("KAFKASERVICE_URI")

CALCULATOR_URI = env_calc if env_calc else "localhost:50052"
KAFKA_URI = env_kafka if env_kafka else "localhost:9092"
# Pakko olla http://, muuten ei toimi
KAFKASERVICE_URI = enc_kafkaservice if enc_kafkaservice else "http://kafkaservice:8084"

@app.get("/")
def index():
    with grpc.insecure_channel(CALCULATOR_URI) as channel:
        stub = calculator_pb2_grpc.CalculatorStub(channel)
        response = stub.Add(calculator_pb2.CalculationRequest(number1=1, number2=2))
        return make_response(
            {"message": "Basic demonstration control API", "'1+2'": response.result}
        )


""" @app.get("/kafka")
def get_kafka():
    consumer = KafkaConsumer("words", bootstrap_servers=KAFKA_URI)
    words = next(consumer).value.decode("utf-8")
    return make_response({"words": words}) """


@app.post("/")
def post_message():
    try:
        message = request.json["message"]
        message = request
        print(message)
        # Pass message onward here
        return make_response({"message": f"processed message '{message}'"})
    except KeyError:
        raise BadRequest()

@app.post("/new_topic")
def new_kafka_topic():
    if request.content_type != 'application/json':
        return make_response("Request content type must be JSON", 415)
    
    new_topic = request.json["topic"]

    try:
        response = requests.post(f"{KAFKASERVICE_URI}/kafka_topic", json={"topic": new_topic})
        return make_response(response.json(), response.status_code)
    except Exception as e:
        return make_response({"error": str(e)}, 500)

# test that kafka works with grpc
@app.post("/send_producer")
def post_message_kafka():
    if request.content_type != 'application/json':
        return make_response("Request content type must be JSON", 415)
    
    data = request.json
    if not data:
        return make_response("Request content type must be JSON", 415)

    topic = data.get("topic")
    one = data.get("number1")
    two = data.get("number2")

    try:
        with grpc.insecure_channel(CALCULATOR_URI) as channel:
            stub = calculator_pb2_grpc.CalculatorStub(channel)
            response = stub.Add(calculator_pb2.CalculationRequest(number1=one, number2=two))
    except grpc.RpcError as e:
        return make_response({"error": f"gRPC error: {e}"}, 500)

    try:
        kafkaResponse = requests.post(f"{KAFKASERVICE_URI}/kafka_producer", json={"topic": topic, "message": response.result})
        kafkaResponse.raise_for_status()
        return make_response(kafkaResponse.json())
    except requests.RequestException as e:
        return make_response({"error": f"Kafka service error: {e}"}, 500)

@app.post("/start_consumer")
def start_consumer():
    if request.content_type != 'application/json':
        return make_response("Request content type must be JSON", 415)

    topic = request.json["topic"]

    try:
        response = requests.post(f"{KAFKASERVICE_URI}/start_consumer", json={"topic": topic})
        return make_response(response.json(), response.status_code)
    except KeyError:
        return make_response({"error": "Bad Request"}), 400

@app.errorhandler(BadRequest)
def handle_bad_request(e):
    return make_response(e.description, 400)
