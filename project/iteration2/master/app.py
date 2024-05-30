from flask import Flask, request, jsonify
import requests
import logging
import os
import uuid
import time
import threading

app = Flask(__name__)
messages = []
SECONDARY_URLS = os.getenv('SECONDARY_URLS', '').split(',')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def replicate_message(secondary, message):
    try:
        response = requests.post(f'{secondary}/replicate', json={'message': message})
        if response.status_code == 200:
            logger.info(f"Replicated message to {secondary} successfully.")
            return True
    except requests.RequestException as e:
        logger.error(f"Error replicating message to {secondary}: {e}")
        time.sleep(1)


@app.route('/messages', methods=['POST'])
def append_message():
    data = request.json
    msg = data.get("msg")
    w = data.get("w", 1)  # Default to 1 if not provided

    if not msg:
        logger.info(f"Message not received")
        return jsonify({'error': "message not received"}), 400
    
    message_id = str(uuid.uuid4())
    message_entry = {"id": message_id, "msg": msg}
    messages.append(message_entry)
    
    if w == 1:
        def replicate_to_all(message_entry):
            for server in SECONDARY_URLS:
                threading.Thread(target=replicate_message, args=(server, message_entry)).start()
        threading.Thread(target=lambda: replicate_to_all(message_entry)).start()
    else:
        required_acks = w - 1
        acks_received = 0
        threads = []

        for server in SECONDARY_URLS:
            thread = threading.Thread(target=replicate_message, args=(server, message_entry))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
            acks_received += 1
            if acks_received >= required_acks:
                break
        
        if acks_received < required_acks:
            return jsonify({'error': 'failed to replicate message to required number of secondaries'}), 500

    return jsonify({'status': 'replication started'}), 200


@app.route('/messages', methods=['GET'])
def get_message():
    logger.info("Fetching all logs.")
    return jsonify(messages), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=25000)
