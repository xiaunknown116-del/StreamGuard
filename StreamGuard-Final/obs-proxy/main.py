"""
Simple OBS proxy placeholder implementation.
Provides a health endpoint and a minimal POST endpoint for proxying commands.
"""
from flask import Flask, jsonify, request

app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'})

@app.route('/proxy/trigger', methods=['POST'])
def trigger():
    data = request.json or {}
    # Placeholder: in a real proxy this would call OBS websocket or other services
    return jsonify({'received': data}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
