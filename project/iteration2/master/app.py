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


def replicate_to_secondary(message, concern):
    acked = 0
    if concern == 1:
        for secondary in SECONDARY_URLS:
            replicate_message(secondary, message)
    else:
        for secondary in SECONDARY_URLS:
            responses = replicate_message(secondary, message)
            if responses:
                acked += 1
            if acked >= concern - 1:
                break


@app.route('/messages', methods=['POST'])
def append_message():
    data = request.json
    msg = data.get("msg")
    w = data.get("w", 1)  # Default to 1 if not provided

    if not msg:
        return "Message not received", 400

    message_id = str(uuid.uuid4())
    message_entry = {"id": message_id, "msg": msg}
    messages.append(message_entry)

    replication_thread = threading.Thread(target=replicate_to_secondary, args=(message_entry, w))
    replication_thread.start()

    return jsonify({'status': 'replication started'}), 200

@app.route('/messages', methods=['GET'])
def get_message():
    logger.info("Fetching all logs.")
    return jsonify(messages), 200

# @app.route('/messages', methods=['GET'])
# def get_messages():
#     all_messages = messages.copy()
#     for secondary_url in SECONDARY_URLS:
#         try:
#             response = requests.get(f"{secondary_url}/messages")
#             if response.status_code == 200:
#                 secondary_messages = response.json()
#                 all_messages.extend(secondary_messages)
#             else:
#                 logger.warning(f"Failed to fetch messages from {secondary_url}.")
#         except requests.RequestException as e:
#             logger.error(f"Error fetching messages from {secondary_url}: {e}")
#     all_messages = list({m["id"]: m for m in all_messages}.values())
#     return jsonify(all_messages)


# if w == 1:
    #     for secondary in SECONDARY_URLS:
    #         try:
    #             response = requests.post(f'{secondary}/replicate', json={'message': message_entry})
    #             if response.status_code == 200:
    #                 logger.info(f"Replicated message to {secondary} successfully.")
    #             else:
    #                 logger.warning(f"Failed to replicate message to {secondary}.")
    #         except requests.RequestException as e:
    #             logger.error(f"Error replicating message to {secondary}: {e}")
    #             time.sleep(1)
    # else:
    #     for secondary in SECONDARY_URLS:
    #         responses = replicate_message(secondary, message_entry)
    #         if responses:
    #             result.append(result)
    #         if len(responses) >= w - 1:
    #             break



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=25000)
