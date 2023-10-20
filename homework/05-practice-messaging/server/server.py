import logging
import pika
import time

from flask import Flask, request
from typing import List, Optional

from config import IMAGES_ENDPOINT, DATA_DIR


class Server:
    # TODO: Your code here.
    def __init__(self, host, port):
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port))
        self._channel = self._connection.channel()
        self._count = 0

    def store_image(self, image: str) -> int:
        self._count += 1

        message = image
        message += '**'
        message += str(self._count)

        self._channel.basic_publish(
                        exchange='',
                        routing_key='server_queue',
                        body=message.encode())
        return self._count

    def get_processed_images(self) -> List[int]:
        ans = []
        bodies = []
        while True:
            method_frame, _, body = self._channel.basic_get(queue='worker_queue')
            if method_frame:
                result = body.decode()
                try:
                    ans.append(int(result.split('**')[0]))
                    bodies.append(body)
                except Exception as e:
                    # print(result)
                    continue   
            else:
                break
        for i in range(len(bodies)):
            self._channel.basic_publish(
                        exchange='',
                        routing_key='worker_queue',
                        body=bodies[i])
        # print(ans)
        return ans
        # raise NotImplementedError

    def get_image_description(self, image_id: str) -> Optional[str]:
        ans = None
        bodies = []
        while True:
            method_frame, _, body = self._channel.basic_get(queue='worker_queue')
            if method_frame:
                result = body.decode()
                try:
                    if image_id == result.split('**')[0]:

                        file_path = f'/data/{image_id}.txt'
                        with open(file_path, 'rb') as file:
                            bytes = file.read()
                            file.close()
                        ans = bytes.decode()
                        # print(ans)
                        # ans = result.split('**')[1]

                    bodies.append(body)
                    if ans != None:
                        break
                except Exception as e:
                    continue   
            else:
                break
        for i in range(len(bodies)):
            self._channel.basic_publish(
                        exchange='',
                        routing_key='worker_queue',
                        body=bodies[i])
        return ans
        # raise NotImplementedError
 

def create_app() -> Flask:
    """
    Create flask application
    """
    app = Flask(__name__)

    server = Server('rabbitmq', 5672)

    @app.route(IMAGES_ENDPOINT, methods=['POST'])
    def add_image():
        body = request.get_json(force=True)
        image_id = server.store_image(body['image_url'])
        return {"image_id": image_id}

    @app.route(IMAGES_ENDPOINT, methods=['GET'])
    def get_image_ids():
        image_ids = server.get_processed_images()
        return {"image_ids": image_ids}

    @app.route(f'{IMAGES_ENDPOINT}/<string:image_id>', methods=['GET'])
    def get_processing_result(image_id):
        result = server.get_image_description(image_id)
        if result is None:
            return "Image not found.", 404
        else:
            return {'description': result}

    return app


app = create_app()

if __name__ == '__main__':
    logging.basicConfig()
    app.run(host='0.0.0.0', port=5000)
