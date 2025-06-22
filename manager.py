import pika
import json
from flask import Flask, request, jsonify
from threading import Thread, Lock
import time

app = Flask(__name__)

active_nodes = {}  # { node_id: node_url }
file_locations = {}  # { filename: node_url }
node_lock = Lock()
node_iterator = iter([])

def get_next_node():
    global node_iterator
    with node_lock:
        nodes = list(active_nodes.values())
        if not nodes:
            return None
        try:
            return next(node_iterator)
        except StopIteration:
            node_iterator = iter(nodes)
            return next(node_iterator)

# RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='manager_queue')

@app.route("/upload_request", methods=["POST"])
def upload_request():
    filename = request.json["filename"]
    node_url = get_next_node()
    if not node_url:
        return "Nenhum nó ativo", 503
    return jsonify({"node_url": node_url})

@app.route("/download_location/<filename>")
def download_location(filename):
    node_url = file_locations.get(filename)
    if node_url:
        return jsonify({"node_url": node_url})
    return "Arquivo não encontrado", 404

@app.route("/list")
def list_files():
    return jsonify(file_locations)

@app.route("/remove/<filename>", methods=["DELETE"])
def remove_file(filename):
    if filename in file_locations:
        del file_locations[filename]
        return "Removido", 200
    return "Arquivo não encontrado", 404

def consume_messages():
    def callback(ch, method, properties, body):
        global node_iterator
        data = json.loads(body)
        if data['type'] == 'register_file':
            file_locations[data['filename']] = data['node_url']
        elif data['type'] == 'heartbeat':
            active_nodes[data['node_id']] = data['node_url']
            node_iterator = iter(list(active_nodes.values()))

    channel.basic_consume(queue='manager_queue', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

Thread(target=consume_messages, daemon=True).start()

if __name__ == "__main__":
    app.run(port=5000)