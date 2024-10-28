# visualization_server.py
from flask import Flask, jsonify
from kafka import KafkaConsumer
import json

app = Flask(__name__)
consumer = KafkaConsumer('advisory_data', 
                         bootstrap_servers='localhost:9092', 
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

@app.route("/advisories", methods=["GET"])
def get_advisories():
    advisories = [msg.value for msg in consumer]
    return jsonify(advisories)

if __name__ == "__main__":
    app.run(debug=True)
