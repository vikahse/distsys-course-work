Для начала я создаю соединение с RabbitMQ в сервере, указывая хост и порт, а затем создается канал, через который будет осуществляться взаимодействие с RabbitMQ.
Так же я завожу поле count для уникальных id. В функции store_image я передаю в очередь server_queue сообщение, содержащее image_url и id. В функции get_processed_images
я пока это возможно достаю из очереди обработанных сообщений worker_queue id изображений. Так же в конце не забываю положить их обратно, так как запросы могут повторяться, а
метод basic_get удаляет элемент из очереди. Тоже самое я проделываю в методе get_image_description, только ищу соотвествующее изображение по id и вывожу его caption. В worker
я так же создаю соединение с RabbitMQ, определяю очереди server_queue и worker_queue с параметром durable=True, чтобы убедиться, что очередь будет сохраняться при перезапуске RabbitMQ.
Затем определяю функцию callback, которая будет вызываться при получении сообщений из очереди server_queue. В этой функции после успешной обработки кладу результат в очередь worker_queue, из
которой в дальнейшем будет считывать сервер. По сути очередь worker_queue - это очередь обработанных сообщений, а server_queue - наоборот. Еще устанавливается настройка prefetch_count=1, чтобы RabbitMQ 
не отправлял более одного сообщения одновременно на обработку. Устанавливается прослушивание очереди server_queue с использованием функции callback и запускается 
бесконечный цикл обработки сообщений из очереди с использованием метода start_consuming(), в случае прерывания соединение закрывается. Благодаря двум очередям, у нас получается
реализовать логику передачи id обработанных изображений напрямую серверу.
