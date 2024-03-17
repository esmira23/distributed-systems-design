from flask import Flask, request, jsonify
import requests
import uuid
import json
from pyotp import random

app = Flask(__name__)

MESSAGES_SERVICE_URL = 'http://localhost:5004/messages'

@app.route('/', methods=['POST'])
def handle_post():
    msg = request.json.get("msg")
    msg_id = str(uuid.uuid4())
    if not msg: 
        return "Message not received", 400
    
    port = random.randint(5001, 5003)
    logging_service_url = f"http://localhost:{port}/logs"

    response = requests.post(logging_service_url, json={'id': msg_id, 'message': msg})
    if response.status_code not in range(200, 300): 
        return "Message not sent", 400

    combined_response = {
        'logging_service': json.loads(response.text),
        'farade_service': {'id': msg_id, 'message': msg}
    }
    return jsonify(combined_response), 200

@app.route('/', methods=['GET'])
def handle_get():
    port = random.randint(5001, 5003)
    logging_service_url = f"http://localhost:{port}/logs"
    
    log_response = requests.get(logging_service_url).json()
    msg_response = requests.get(MESSAGES_SERVICE_URL).json()
    return [log_response, msg_response], 200

if __name__ == '__main__':
    app.run(port=5000)
