from flask import Flask, request, jsonify
import requests
import logging
import os
import time


app = Flask(__name__)
messages = []
SECONDARY_URLS = os.getenv('SECONDARY_URLS', '').split(',')
# secondaries = ["http://secondary1:5001", "http://secondary2:5002"]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@app.route('/messages', methods=['POST'])
def append_message():
    msg = request.json.get("msg")

    if not msg: 
        return "Message not received", 400

    messages.append(msg)
    acks = []

    for secondary in SECONDARY_URLS:
        try:
            response = requests.post(f'{secondary}/replicate', json={'message': msg})
            if response.status_code == 200:
                logger.info(f"Replicated message to {secondary} successfully.")
                acks.append(True)
            else:
                logger.warning(f"Failed to replicate message to {secondary}.")
                acks.append(False)
        except requests.RequestException as e:
            logger.error(f"Error replicating message to {secondary}: {e}")
            time.sleep(1)
    if all(acks):
        return jsonify({"status": "success"}), 200
    else:
        return jsonify({"status": "failure"}), 500


@app.route('/messages', methods=['GET'])
def get_message():
    logger.info("Fetching all logs.")
    return jsonify(messages), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=25000)
