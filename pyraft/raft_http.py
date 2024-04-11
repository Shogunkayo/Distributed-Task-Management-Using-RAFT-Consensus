from flask import Flask, jsonify, request
import threading
import raft
import redis
import argparse

# if addr is 127.0.0.1:x
# raft client runs on port x
# raft internal working runs on port x + 1
# flask http server runs on port x + 1000

parser = argparse.ArgumentParser()

parser.add_argument('-a', dest='addr', help='ip:port[port+1], :port means pick one ip by gethostbyname (ex. -a 127.0.0.1:5010)')
parser.add_argument('-e', dest='ensemble', help='ensemble ip list or domain name with port (ex. -e 2/127.0.0.1:5020,3/127.0.0.1:5030 or -e 127.0.0.1:5020,127.0.0.1:5030 or -e pyraft.test.com:5010)')
parser.add_argument('-i', dest='nid', help='self node id (if not exists, use address, HOSTNAME use machine hostname) (ex. -i 1)')
parser.add_argument('-l', dest='load', help='checkpoint filename to load')
parser.add_argument('-o', dest='overwrite_peer', help='overwrite duplicated nid node (delete previous one)', action='store_true')
parser.add_argument('-loglevel', dest='loglevel', default='info', help='loglevel (debug, info, warning, error, fatal)')
parser.add_argument('-logfile', dest='logfile', help='logger rotation file')

args = parser.parse_args()

ip, port = args.addr.split(':')
port = int(port)

print("PORT", port)

r = redis.Redis(host="127.0.0.1", port=port, db=0)

broker_epoch_lock = threading.Lock()
broker_status_lock = threading.Lock()
app = Flask(__name__)

# Create a Raft node
node = raft.make_default_node(args)

def start_flask_app(port):
    app.run(port=port)

def start_flask_app_on_port(port):
    flask_thread = threading.Thread(target=start_flask_app, args=(port,))
    flask_thread.start()

def on_start_handler(node):
    start_flask_app_on_port(port + 1000)

# Register on_start_handler for the on_start event
node.worker.handler['on_start'] = on_start_handler

@app.route('/status', methods=['GET'])
def get_status():
    status = {'node_id': node.nid, 'state': node.state}
    return jsonify(status)

@app.route('/store', methods=['POST'])
def store_data():
    data = request.get_json()
    key = data.get('key')
    value = data.get('value')

    if key and value:
        r.set(key, value)
        return jsonify({'message': f'Key-value pair ({key}: {value}) stored successfully'})
    else:
        return jsonify({'error': 'Invalid data provided'})

@app.route('/retrieve/<key>', methods=['GET'])
def retrieve_data(key):
    value = r.get(key)
    if value:
        return jsonify({'key': key, 'value': value.decode()})
    else:
        return jsonify({'error': 'Key not found'})

# Start the Raft node
node.start()
node.join()
