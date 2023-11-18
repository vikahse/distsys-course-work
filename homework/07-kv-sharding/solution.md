Когда приходит запрос PUT key value, то я сначала хеширую key, а потом отправляю его на тот шард, у которого с ним минимальная разница между hash_key и id (это описано в методе get_id_node).
Если мы сейчас не находимся в этой ноде, то отправляем ей сообщение PUT и просим так же изначальную ноду отправить локально подтверждение с помощью сообщения PUT_RESP.

Когда приходит запрос GET key, то я так же с помощью метода get_id_node ищу номер ноды, где хранится данный key и отправляю этому шарду GET запрос с последующей передачей value через 
GET_RESP сообщения. 

Когда приходит запрос DELETE key, то я с помощью метода get_id_node ищу номер ноды, где хранится данный key и отправляю этому шарду DELETE запрос с последующей передачей value через 
DELETE_RESP сообщения. 

Когда приходит запрос NODE_ADDED id, я добавляю id в массив nodes и провожу стабилизацию, таким образом, что каждый шард заново достает свои ключи и с помощью метода get_id_node проверяет
принадлежит ли данный ключ все еще ему, иначе удаляет его и отправляет на добавление соответствующему узлу.

Когда приходит запрос NODE_REMOVED id, я удаляю id из массива nodes и провожу аналогичную стабилизацию, описанную выше.

Так же я пыталась использовать таймеры для переодической стабилизации, чтобы было равномерное распределение, но этот метод не сильно помог решить проблему с DISTRIBUTION.