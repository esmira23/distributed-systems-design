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


def replicate_message(server, message, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            response = requests.post(f'{server}/replicate', json={'message': message})
            if response.status_code == 200:
                logger.info(f"Message replicated to {server}")
                return True
        except requests.RequestException as e:
            logger.error(f"Error replicating to {server}: {e}")
        retries += 1
        time.sleep(2**retries)  # Exponential backoff

    logger.error(f"Failed to replicate message to {server} after {max_retries} retries")
    return False


@app.route('/messages', methods=['POST'])
def append_message():
    data = request.json
    msg = data.get("msg")
    w = data.get("w", 1)  # Default to 1 if not provided

    if not msg:
        logger.info(f"Message not received")
        return jsonify({'error': "message not received"}), 400
    

    if not any(message['msg'] == msg for message in messages):
        message_id = str(uuid.uuid4())
        message_entry = {"id": message_id, "msg": msg}
        messages.append(message_entry)
    else:
        return jsonify({'status': "message already exists"}), 200

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
def get_messages():
    all_messages = messages.copy()
    for secondary_url in SECONDARY_URLS:
        try:
            response = requests.get(f"{secondary_url}/messages")
            if response.status_code == 200:
                secondary_messages = response.json()
                all_messages.extend(secondary_messages)
            else:
                logger.warning(f"Failed to fetch messages from {secondary_url}.")
        except requests.RequestException as e:
            logger.error(f"Error fetching messages from {secondary_url}: {e}")

    # Deduplicate and sort messages by ID to maintain total order
    unique_messages = {message['id']: message for message in all_messages}
    sorted_messages = sorted(unique_messages.values(), key=lambda x: x['msg'])
    
    return jsonify(sorted_messages), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=25000)
