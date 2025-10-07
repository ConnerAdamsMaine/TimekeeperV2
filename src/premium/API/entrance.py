import flask
from flask import Flask, jsonify
import asyncio

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

@app.route('/status', methods=['GET'])
def status_root():
    return jsonify({"status": "ok"})

@app.route('/status/<server_id>', methods=['GET'])
def status_server(server_id):
    pass