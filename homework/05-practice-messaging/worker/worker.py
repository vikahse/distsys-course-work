from caption import get_image_caption
import pika
import time
import os

if __name__ == '__main__':
    # #TODO: Your code here.
    # # Register consumer and implement message processing.
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', port=5672))
    channel = connection.channel()

    channel.queue_declare(queue='server_queue', durable=True)
    channel.queue_declare(queue='worker_queue', durable=True)

    def callback(ch, method, properties, body):
        try:
            result = body.decode()
            
            caption = get_image_caption(result.split('**')[0])
            id = result.split('**')[1]

            file_path = f'/data/{id}.txt'
            with open(file_path, 'w') as file:
                file.write(caption)
                file.close()
                    
            message = id
            message += '**'
            message += caption


            ch.basic_publish(exchange='',
                            routing_key='worker_queue',
                            body=message.encode())
            
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    # def call(ch, method, properties, body):
    #     print('someone', body)
    
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='server_queue', on_message_callback=callback)
    # channel.basic_consume(queue='worker_queue', on_message_callback=call)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        connection.close()



