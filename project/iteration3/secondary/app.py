import os
import time
import logging
import threading
from flask import Flask, request, jsonify


app = Flask(__name__)
replicated_messages = []
replication_lock = threading.Lock()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
SECONDARY_PORT = os.getenv('PORT', '')

logger = logging.getLogger(__name__)


@app.route('/replicate', methods=['POST'])
def replicate():
    message = request.json.get('message')
    time.sleep(5)  # Simulate delay for inconsistency

    # Deduplicate messages
    with replication_lock:
        if message not in replicated_messages:
            replicated_messages.append(message)
            logger.info(f"Replicated message: {message}")
        else:
            logger.info(f"Duplicate message received: {message}")

    return jsonify({"status": "acknowledged"}), 200


@app.route('/messages', methods=['GET'])
def get_replicated_logs():
    logger.info("Fetching all replicated logs.")
    with replication_lock:
        sorted_messages = sorted(replicated_messages, key=lambda x: x['msg'])
    return jsonify(sorted_messages), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=SECONDARY_PORT)
