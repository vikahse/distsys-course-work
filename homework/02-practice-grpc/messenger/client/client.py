import copy
import json
import os
import random
import threading
from http import HTTPStatus
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import List, Dict
from datetime import datetime

import grpc
import google.protobuf.json_format  # ParseDict, MessageToDict
import google.protobuf.empty_pb2  # Empty
from messenger.proto import messenger_pb2
from messenger.proto import messenger_pb2_grpc

class PostBox:
    def __init__(self):
        self._messages: List[Dict] = []
        self._lock = threading.Lock()

    def collect_messages(self) -> List[Dict]:
        with self._lock:
            messages = copy.deepcopy(self._messages)
            self._messages = []
        return messages

    def put_message(self, message: Dict):
        with self._lock:
            self._messages.append(message)


class MessageHandler(BaseHTTPRequestHandler):
    _stub = None
    _postbox: PostBox

    def _read_content(self):
        content_length = int(self.headers['Content-Length'])
        bytes_content = self.rfile.read(content_length)
        return bytes_content.decode('ascii')

    # noinspection PyPep8Naming
    def do_POST(self):
        if self.path == '/sendMessage':
            response = self._send_message(self._read_content())
        elif self.path == '/getAndFlushMessages':
            response = self._get_messages()
        else:
            self.send_error(HTTPStatus.NOT_IMPLEMENTED)
            self.end_headers()
            return
        response_bytes = json.dumps(response).encode('ascii')
        self.send_response(HTTPStatus.OK)
        self.send_header('Content-Length', str(len(response_bytes)))
        self.end_headers()
        self.wfile.write(response_bytes)

    def _send_message(self, content: str) -> dict:
        json_request = json.loads(content)
        # TODO: use google.protobuf.json_format.ParseDict
        message = google.protobuf.json_format.ParseDict(json_request, messenger_pb2.SendRequest())
        # TODO: your rpc call of the messenger here
        send_response = self._stub.SendMessage(message)
        # TODO: use google.protobuf.json_format.MessageToDict here
        read_response = google.protobuf.json_format.MessageToDict(send_response)
        return {'sendTime': read_response['sendTime']}

    def _get_messages(self) -> List[dict]:
        return self._postbox.collect_messages()

def messages_stream(stub, postbox):
    while 1:
        response = stub.ReadMessages(messenger_pb2.ReadRequest())
        for message in response:
            d = {}
            d['author'] = message.author
            d['text'] = message.text
            datetime_object = message.sendTime.ToDatetime()
            d['sendTime'] = datetime_object.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            postbox.put_message(d)

def main():
    grpc_server_address = os.environ.get('MESSENGER_SERVER_ADDR', 'localhost:51075')

    # TODO: create your grpc client with given address
    stub = messenger_pb2_grpc.MessengerServerStub(grpc.insecure_channel(grpc_server_address))
    # A list of messages obtained from the server-py but not yet requested by the user to be shown
    # (via the http's /getAndFlushMessages).
    postbox = PostBox()

    # TODO: Implement and run a messages stream consumer in a background thread here.
    # It should fetch messages via the grpc client and store them in the postbox.
    thread = threading.Thread(target=messages_stream, args=(stub, postbox))
    thread.start()
    
    # Pass the stub and the postbox to the HTTP server.
    # Dirty, but this simple http server doesn't provide interface
    # for passing arguments to the handler c-tor.
    MessageHandler._stub = stub
    MessageHandler._postbox = postbox

    http_port = os.environ.get('MESSENGER_HTTP_PORT', '8080')
    http_server_address = ('0.0.0.0', int(http_port))

    # NB: handler_class is instantiated for every http request. Do not store any inter-request state in it.
    httpd = HTTPServer(http_server_address, MessageHandler)
    httpd.serve_forever()

    thread.join()


if __name__ == '__main__':
    main()
