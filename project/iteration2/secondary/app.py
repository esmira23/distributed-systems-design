from flask import Flask, request, jsonify
import os
import time
import logging


app = Flask(__name__)
replicated_messages = []
SECONDARY_PORT = os.getenv('PORT', '')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@app.route('/replicate', methods=['POST'])
def replicate():
    message = request.json.get('message')
    time.sleep(5)  # Simulate delay for inconsistency
    # Deduplicate messages
    if message not in replicated_messages:
        replicated_messages.append(message)
        logger.info(f"Replicated message: {message}")
    return jsonify({"status": "acknowledged"}), 200


@app.route('/messages', methods=['GET'])
def get_replicated_logs():
    logger.info("Fetching all replicated logs.")
    return jsonify(replicated_messages), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=SECONDARY_PORT)
