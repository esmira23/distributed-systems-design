from flask import Flask, jsonify
import hazelcast
import argparse
import hazelcast.listener
from threading import Thread


parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, required=True)
args = parser.parse_args()

hz = hazelcast.HazelcastClient(cluster_name="dev", cluster_members=[])
msg_queue = hz.get_queue("message-queue").blocking()
messages = []

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
