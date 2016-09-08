import kombu
from bottle import post, get, request, run, response
import json
import os
import yaml
from base64 import b64encode

conf_dir = os.environ.get('HARDWARE_API_CONFIG_DIR', '../')

with open(conf_dir + 'config.yaml') as config:
     conf = yaml.load(config)

out_routes = {}

for k,v in conf['routes'].iteritems():
    hosts = v.get('hosts',['localhost'])
    uri = '{}://{}:{}@{}:{}/{}'.format(v.get('protocol','amqp'),
                                    v.get('username','guest'),
                                    v.get('password','guest'),
                                    hosts[0],
                                    v.get('port', '5672'),
                                    v.get('vhost','/'))
    conn, channel, sqs = None, None, None
    try:

        conn = kombu.Connection(uri)
        conn.connect()
        channel = conn.channel()
        sqs = conn.SimpleQueue(v.get('queue','unknown'))
    except conn.connection_errors + conn.channel_errors:
        print("couldn't connect, oh well.")
    out_routes[k] = { 'connection': conn, 'channel': channel, 'queue': sqs}

def close_routes():
    for k,v in out_routes.iteritems():
        conn = v.get('connection')
        print('Attempting to close route: {}'.format(k))
        conn.close()

@post('/v3')
def is_v3():
    serviceid = request.headers.get('X-Zonar-Service')
    if serviceid is None:
        response.status = 400
        response.body = "No Zonar Header"

    route = out_routes.get(serviceid, None)
    if route is None:
        response.status = 400
        response.body = "Can not route Zonar Service"
    
    message = dict(request.headers)
    for k,v in request.files.iteritems():
        print(k)
        mesg = request.files.get(k)
        message[k] = b64encode(mesg.file.read())
    return json.dumps(dict(message))

@get('/end')
def is_end():
    close_routes()
    return "All gone."

run(host='localhost', port=8080, debug=True)