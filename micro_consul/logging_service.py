from flask import Flask, request, jsonify
import hazelcast
import argparse
import my_consul
import subprocess
import json


parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, required=True)
args = parser.parse_args()

p = subprocess.Popen(["hz", "start"])

logging_id = my_consul.register_service('logging-service', args.port)

hz_configs_json = my_consul.get_key_value('hz_configs')
if hz_configs_json:
    hz_config = json.loads(hz_configs_json)
    print("Hazelcast configuration: ", hz_config)
else:
    print("Hazelcast configuration not found in Consul.")
    exit(1)

hz = hazelcast.HazelcastClient(cluster_name=hz_config['cluster_name'], cluster_members=hz_config['cluster_members'])
logs_msg = hz.get_map(hz_config['map_name']).blocking()


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

    input("Press Enter to exit...\n")
    my_consul.deregister_service(logging_id)
