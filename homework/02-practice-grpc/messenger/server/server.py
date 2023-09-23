import grpc
import os
from concurrent import futures
from messenger.proto import messenger_pb2
from messenger.proto import messenger_pb2_grpc
import time
from google.protobuf.timestamp_pb2 import Timestamp

class MessengerServer(messenger_pb2_grpc.MessengerServer):
    def __init__(self):
        self.messages = []

    def SendMessage(self, request, context):
        send_time = Timestamp()
        send_time.GetCurrentTime()
        # send_request_message = messenger_pb2.SendRequest(author=request.author, text=request.text)
        read_response_message = messenger_pb2.ReadResponse(author=request.author, text=request.text, sendTime=send_time)
        self.messages.append(read_response_message)
        return messenger_pb2.SendResponse(sendTime=send_time)

    def ReadMessages(self, request, context):
        client = len(self.messages)
        while 1:
            new_length = len(self.messages)
            if new_length > client:
                for i in range(client, new_length):
                    yield self.messages[i]
                client = new_length
    
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    messenger_pb2_grpc.add_MessengerServerServicer_to_server(MessengerServer(), server)
    server.add_insecure_port('0.0.0.0:{}'.format(os.environ['MESSENGER_SERVER_PORT']))
    # server.add_insecure_port('0.0.0.0:51075')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()


