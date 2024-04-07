from flask import Flask, request, jsonify
import hazelcast
import argparse
import subprocess


parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, required=True)
args = parser.parse_args()

p = subprocess.Popen(["hz", "start"])

hz = hazelcast.HazelcastClient(cluster_name="dev", cluster_members=[])
logs_msg = hz.get_map("messages").blocking()

app = Flask(__name__)

@app.route('/logs', methods=['POST'])
def log_message():
    data = request.json
    msg_id = data['id']
    msg = data['message']
    
    logs_msg.put(msg_id, msg)
    print(f'[logged message] : {msg}')
    return jsonify({'status': 'success'}), 200

@app.route('/logs', methods=['GET'])
def get_logs():
    all_msg = {key: logs_msg.get(key) for key in logs_msg.key_set()}
    return jsonify(all_msg), 200

if __name__ == '__main__':
    app.run(port=args.port)
