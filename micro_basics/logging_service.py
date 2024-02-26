from flask import Flask, request, jsonify

app = Flask(__name__)
logs = {}

@app.route('/logs', methods=['POST'])
def log_message():
    data = request.json
    msg_id = data['id']
    msg = data['message']
    logs[msg_id] = msg
    print(f'[logged message] : {msg}')
    return jsonify({'status': 'success'}), 200

@app.route('/logs', methods=['GET'])
def get_logs():
    return jsonify(logs), 200

if __name__ == '__main__':
    app.run(port=5001)
