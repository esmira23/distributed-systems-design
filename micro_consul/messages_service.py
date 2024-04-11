from flask import Flask, jsonify
from threading import Thread
import hazelcast
import argparse
import my_consul
import json


parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, required=True)
args = parser.parse_args()
messages = []

message_id = my_consul.register_service('messages-service', args.port)

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

def consume_messages():
    while True:
        message = msg_queue.take()
        messages.append(message)
        print("Received message:", message)

consume_thread = Thread(target=consume_messages)
consume_thread.daemon = True
consume_thread.start()


app = Flask(__name__)
        
@app.route('/messages', methods=['GET'])
def static_message():
    if messages:
        return jsonify({f"messages from port {args.port}": messages}), 200
    else:
        return "There is no messages"

if __name__ == '__main__':
    app.run(port=args.port)

    input("Press Enter to exit...\n")
    my_consul.deregister_service(message_id)
