import os
import threading
import time
import pika
import json
from flask import Flask, request, send_file

NODE_ID = os.environ.get("NODE_ID", "node1")
NODE_URL = os.environ.get("NODE_URL", "http://localhost:5001")
STORAGE_DIR = "storage"
os.makedirs(STORAGE_DIR, exist_ok=True)

app = Flask(__name__)

# Configuração RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='manager_queue')

@app.route("/upload", methods=["POST"])
def upload():
    file = request.files["file"]
    filename = request.form["filename"]
    file.save(os.path.join(STORAGE_DIR, filename))

    data = {
        "type": "register_file",
        "filename": filename,
        "node_url": NODE_URL
    }
    channel.basic_publish(exchange='', routing_key='manager_queue', body=json.dumps(data))
    return "Arquivo recebido", 200

@app.route("/download/<filename>")
def download(filename):
    return send_file(os.path.join(STORAGE_DIR, filename))

def send_heartbeat():
    while True:
        try:
            data = {
                "type": "heartbeat",
                "node_id": NODE_ID,
                "node_url": NODE_URL
            }
            channel.basic_publish(exchange='', routing_key='manager_queue', body=json.dumps(data))
        except:
            pass
        time.sleep(5)

threading.Thread(target=send_heartbeat, daemon=True).start()

if __name__ == "__main__":
    porta = int(NODE_URL.split(":")[-1])
    app.run(port=porta)
