from dslabmp import Context, Message, Process
import uuid


# AT MOST ONCE ---------------------------------------------------------------------------------------------------------

class AtMostOnceSender(Process):
    def __init__(self, proc_id: str, receiver_id: str):
        self._id = proc_id
        self._receiver = receiver_id
        self._count = 0
        # self._unique_keys = []

    def on_local_message(self, msg: Message, ctx: Context):
        # receive message for delivery from local user

        # изначально генерировала ключи через uuid
        # message_id = str(uuid.uuid4())
        # if len(message_id) > 50:
        #     message_id = message_id[:50]
        # while message_id in self._unique_keys:
        #     message_id = str(uuid.uuid4())
        #     if len(message_id) > 50:
        #         message_id = message_id[:50]
        # self._unique_keys.add(message_id)

        # message_id = self._count
        # self._count += 1

        # пробовала записывать ключ в самом начале сообщения
        # msg_with_key = str(message_id)
        # msg_with_key += 'K'
        # msg_with_key += msg._data['text']
        # msg._data['text'] = msg_with_key

        # один из вариантов, как я пробовала отправлять ключ вместе с сообщением
        # sender_msg = Message(str(message_id), [msg._type, msg._data])

        self._count += 1
        msg._data['key'] = str(self._count)
        ctx.send(msg, self._receiver)
        # ctx.send(sender_msg, self._receiver)


    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver here
        pass

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass

        
class AtMostOnceReceiver(Process):
    def __init__(self, proc_id: str):
        self._id = proc_id
        self._unique_keys = []

    def on_local_message(self, msg: Message, ctx: Context):
        # not used in this task
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver
        # deliver message to local user with ctx.send_local()

        # обработка варианта, когда я записывала ключ вначале сообщения
        # i = 0
        # key = ''
        # while msg._data['text'][i] != 'K':
        #     key += msg._data['text'][i]
        #     i += 1
        
        if msg._data['key'] not in self._unique_keys:
        # if msg._type not in self._unique_keys:
            self._unique_keys.append(msg._data['key'])
            # self._unique_keys.append(msg._type)
            # msg._data['text'] = msg._data['text'][len(key) + 1 :]
            # receiver_msg = Message(msg._data[0], msg._data[1])
            msg._data.pop('key')
            ctx.send_local(msg)
            # ctx.send_local(receiver_msg)
        # pass

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass


# AT LEAST ONCE --------------------------------------------------------------------------------------------------------

class AtLeastOnceSender(Process):
    def __init__(self, proc_id: str, receiver_id: str):
        self._id = proc_id
        self._receiver = receiver_id
        self._not_finished = {}
        # self._finished = []
        self._count = 0 

    def on_local_message(self, msg: Message, ctx: Context):
        # receive message for delivery from local user

        # message_id = str(uuid.uuid4())
        # if len(message_id) > 50:
        #     message_id = message_id[:50]
        # while message_id in self._finished:
        #     message_id = str(uuid.uuid4())
        #     if len(message_id) > 50:
        #         message_id = message_id[:50]

        # message_id = self._count
        # self._count += 1

        self._count += 1
        msg._data['key'] = str(self._count)

        self._not_finished[str(self._count)] = msg

        # sender_msg = Message(str(message_id), [msg._type, msg._data])
        # ctx.send(sender_msg, self._receiver)

        ctx.send(msg, self._receiver)
        ctx.set_timer(str(self._count), 3) 

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver here
        # if msg._type in self._not_finished.keys():
        if msg._data['key'] in self._not_finished.keys():
            # ctx.cancel_timer(msg._type)
            ctx.cancel_timer(msg._data['key'])
            # self._not_finished.pop(msg._type)
            self._not_finished.pop(msg._data['key'])
            # self._finished.append(msg._type)

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        if timer_name in self._not_finished.keys():
            # sender_msg = Message(timer_name, [self._not_finished[timer_name]._type, self._not_finished[timer_name]._data])
            ctx.send(self._not_finished[timer_name], self._receiver)
            ctx.set_timer(timer_name, 3)

class AtLeastOnceReceiver(Process):
    def __init__(self, proc_id: str):
        self._id = proc_id

    def on_local_message(self, msg: Message, ctx: Context):
        # not used in this task
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver
        # deliver message to local user with ctx.send_local()
        # receiver_msg = Message(msg._data[0], msg._data[1])
        ctx.send(msg, sender) 
        msg._data.pop('key')
        ctx.send_local(msg)
        # ctx.send_local(receiver_msg)

        # ans = Message(msg._type, msg._data)
        # ctx.send(ans, sender) 

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass


# EXACTLY ONCE ---------------------------------------------------------------------------------------------------------

class ExactlyOnceSender(Process):
    def __init__(self, proc_id: str, receiver_id: str):
        self._id = proc_id
        self._receiver = receiver_id
        self._not_finished = {}
        # self._finished = []
        self._count = 0 

    def on_local_message(self, msg: Message, ctx: Context):
        # receive message for delivery from local user

        # message_id = str(uuid.uuid4())
        # if len(message_id) > 50:
        #     message_id = message_id[:50]
        # while message_id in self._finished:
        #     message_id = str(uuid.uuid4())
        #     if len(message_id) > 50:
        #         message_id = message_id[:50]

        # message_id = self._count
        # self._count += 1

        self._count += 1
        msg._data['key'] = str(self._count)

        self._not_finished[str(self._count)] = msg

        # sender_msg = Message(str(message_id), [msg._type, msg._data])
        # ctx.send(sender_msg, self._receiver)
        
        ctx.send(msg, self._receiver)
        ctx.set_timer(str(self._count), 3) 

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver here
        # if msg._type in self._not_finished.keys():
        if msg._data['key'] in self._not_finished.keys():
            # ctx.cancel_timer(msg._type)
            ctx.cancel_timer(msg._data['key'])
            # self._not_finished.pop(msg._type)
            self._not_finished.pop(msg._data['key'])
            # self._finished.append(msg._type)

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        if timer_name in self._not_finished.keys():
            # sender_msg = Message(timer_name, [self._not_finished[timer_name]._type, self._not_finished[timer_name]._data])
            ctx.send(self._not_finished[timer_name], self._receiver)
            ctx.set_timer(timer_name, 3)


class ExactlyOnceReceiver(Process):
    def __init__(self, proc_id: str):
        self._id = proc_id
        self._unique_keys = []

    def on_local_message(self, msg: Message, ctx: Context):
        # not used in this task
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver
        # deliver message to local user with ctx.send_local()
        if msg._data['key'] not in self._unique_keys:
            ctx.send(msg, sender) 
            self._unique_keys.append(msg._data['key'])
            msg._data.pop('key')
            ctx.send_local(msg)
        else:
            ctx.send(msg, sender) 

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass


# EXACTLY ONCE + ORDERED -----------------------------------------------------------------------------------------------

class ExactlyOnceOrderedSender(Process):
    def __init__(self, proc_id: str, receiver_id: str):
        self._id = proc_id
        self._receiver = receiver_id
        self._not_finished = {}
        self._finished = []
        self._count = 0

    def on_local_message(self, msg: Message, ctx: Context):
        # receive message for delivery from local user
        message_id = self._count + 1
        self._count += 1

        self._not_finished[str(message_id)] = msg
        sender_msg = Message(str(message_id), [msg._type, msg._data])
        ctx.send(sender_msg, self._receiver)
        ctx.set_timer(str(message_id), 3) 
        # pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver here
        if msg._type != 'retry' and msg._type not in self._finished:
            ctx.cancel_timer(msg._type)
            self._not_finished.pop(msg._type)
            self._finished.append(msg._type)
        elif msg._type == 'retry' and msg._data in self._not_finished.keys():
            sender_msg = Message(msg._data, [self._not_finished[msg._data]._type, self._not_finished[msg._data]._data])
            ctx.send(sender_msg, self._receiver)
            ctx.set_timer(msg._data, 3)
        # pass

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        if timer_name in self._not_finished.keys():
            sender_msg = Message(timer_name, [self._not_finished[timer_name]._type, self._not_finished[timer_name]._data])
            ctx.send(sender_msg, self._receiver)
            ctx.set_timer(timer_name, 3)
        # pass

class ExactlyOnceOrderedReceiver(Process):
    def __init__(self, proc_id: str):
        self._id = proc_id
        self._unique_keys = set()
        self._last_delivered_index = 0

    def on_local_message(self, msg: Message, ctx: Context):
        # not used in this task
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver
        # deliver message to local user with ctx.send_local()
        if msg._type not in self._unique_keys:
            if int(msg._type) - 1 == self._last_delivered_index:
                self._last_delivered_index = int(msg._type)

                self._unique_keys.add(msg._type)
                receiver_msg = Message(msg._data[0], msg._data[1])
                ctx.send_local(receiver_msg)

                ans = Message(msg._type, msg._data)
                ctx.send(ans, sender) 
            else:
                for i in range(self._last_delivered_index + 1, int(msg._type) + 1):
                    ans = Message('retry', str(i))
                    ctx.send(ans, sender) 
        else:
            ans = Message(msg._type, msg._data)
            ctx.send(ans, sender) 
        # pass

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass

# class ExactlyOnceOrderedSender(Process):
#     def __init__(self, proc_id: str, receiver_id: str):
#         self._id = proc_id
#         self._receiver = receiver_id
#         self._not_finished = {}
#         # self._finished = []
#         self._count = 0

#     def on_local_message(self, msg: Message, ctx: Context):
#         # receive message for delivery from local user

#         # message_id = self._count + 1
#         # self._count += 1

#         self._count += 1
#         msg._data['key'] = str(self._count)

#         self._not_finished[str(self._count)] = msg
#         # sender_msg = Message(str(message_id), [msg._type, msg._data])
#         # ctx.send(sender_msg, self._receiver)
#         ctx.send(msg, self._receiver)
#         ctx.set_timer(str(self._count), 3) 
#         # pass

#     def on_message(self, msg: Message, sender: str, ctx: Context):
#         # process messages from receiver here
#         if msg._data.get('retry') == None and msg._data['key'] in self._not_finished.keys():
#             ctx.cancel_timer(msg._data['key'])
#             self._not_finished.pop(msg._data['key'])
#             # self._finished.append(msg._type)
#         elif msg._data.get('retry') != None and msg._data['key'] in self._not_finished.keys():
#             # sender_msg = Message(msg._data, [self._not_finished[msg._data]._type, self._not_finished[msg._data]._data])
#             msg._data.pop('retry')
#             ctx.send(msg, self._receiver)
#             ctx.set_timer(msg._data['key'], 3)
#         # pass

#     def on_timer(self, timer_name: str, ctx: Context):
#         # process fired timers here
#         if timer_name in self._not_finished.keys():
#             # sender_msg = Message(timer_name, [self._not_finished[timer_name]._type, self._not_finished[timer_name]._data])
#             ctx.send(self._not_finished[timer_name], self._receiver)
#             ctx.set_timer(timer_name, 3)
#         # pass

# class ExactlyOnceOrderedReceiver(Process):
#     def __init__(self, proc_id: str):
#         self._id = proc_id
#         self._unique_keys = []
#         self._last_delivered_index = 0

#     def on_local_message(self, msg: Message, ctx: Context):
#         # not used in this task
#         pass

#     def on_message(self, msg: Message, sender: str, ctx: Context):
#         # process messages from receiver
#         # deliver message to local user with ctx.send_local()
#         # if msg._type not in self._unique_keys:
#         if msg._data['key'] not in self._unique_keys:
#             if int(msg._data['key']) - 1 == self._last_delivered_index:
#                 self._last_delivered_index = int(msg._data['key'])

#                 self._unique_keys.append(msg._data['key'])
#                 # receiver_msg = Message(msg._data[0], msg._data[1])
#                 ctx.send(msg, sender)
#                 msg._data.pop('key')
#                 ctx.send_local(msg)

#                 # ans = Message(msg._type, msg._data)
#                 # ctx.send(ans, sender) 
#             else:
#                 for i in range(self._last_delivered_index + 1, int(msg._data['key']) + 1):
#                     # ans = Message('retry', str(i))
#                     msg._data['retry'] = str(i)
#                     ctx.send(msg, sender) 
#         else:
#             # ans = Message(msg._type, msg._data)
#             # ctx.send(ans, sender) 
#             ctx.send(msg, sender)
#         # pass

#     def on_timer(self, timer_name: str, ctx: Context):
#         # process fired timers here
#         pass
