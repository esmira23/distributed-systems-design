from flask import Flask, request, jsonify
import requests
import hazelcast
import argparse
import my_consul
import uuid
import json


parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, required=True)
args = parser.parse_args()

facade_id = my_consul.register_service('facade-service', args.port)

hz_configs_json = my_consul.get_key_value('hz_configs')
if hz_configs_json:
    hz_config = json.loads(hz_configs_json)
    print("Hazelcast configuration: ", hz_config)
else:
    print("Hazelcast configuration not found in Consul.")
    exit(1)

mq_configs_json = my_consul.get_key_value('mq_configs')
if mq_configs_json:
    mq_config = json.loads(mq_configs_json)
    print("Message Queue configuration: ", mq_config)
else:
    print("Message Queue configuration not found in Consul.")
    exit(1)

hz = hazelcast.HazelcastClient(cluster_name=hz_config['cluster_name'], cluster_members=hz_config['cluster_members'])
msg_queue = hz.get_queue(mq_config['queue_name']).blocking()


app = Flask(__name__)

@app.route('/', methods=['POST'])
def handle_post():
    logging_service = my_consul.get_service_address_port('logging-service')

    msg = request.json.get("msg")
    msg_id = str(uuid.uuid4())
    if not msg: 
        return "Message not received", 400
    
    logging_service_url = f"{logging_service}/logs"

    response = requests.post(logging_service_url, json={'id': msg_id, 'message': msg})
    if response.status_code not in range(200, 300): 
        return "Message not sent", 400
    msg_queue.offer(msg)

    combined_response = {
        'logging_service': json.loads(response.text),
        'farade_service': {'id': msg_id, 'message': msg}
    }
    return jsonify(combined_response), 200

@app.route('/', methods=['GET'])
def handle_get():
    logging_service = my_consul.get_service_address_port('logging-service')
    message_service = my_consul.get_service_address_port('messages-service')

    logging_service_url = f"{logging_service}/logs"
    log_response = requests.get(logging_service_url).json()

    msg_service_url = f"{message_service}/messages"
    msg_response = requests.get(msg_service_url).json()

    return jsonify(log_response, msg_response), 200

if __name__ == '__main__':
    app.run(port=args.port)

    input("Press Enter to exit...\n")
    my_consul.deregister_service(facade_id)
