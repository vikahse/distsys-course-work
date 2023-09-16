from dslabmp import Context, Message, Process
import uuid


# AT MOST ONCE ---------------------------------------------------------------------------------------------------------

class AtMostOnceSender(Process):
    def __init__(self, proc_id: str, receiver_id: str):
        self._id = proc_id
        self._receiver = receiver_id
        self._count = 0
        # self._unique_keys = set()

    def on_local_message(self, msg: Message, ctx: Context):
        # receive message for delivery from local user

        # message_id = str(uuid.uuid4())
        # if len(message_id) > 50:
        #     message_id = message_id[:50]
        # while message_id in self._unique_keys:
        #     message_id = str(uuid.uuid4())
        #     if len(message_id) > 50:
        #         message_id = message_id[:50]
        # self._unique_keys.add(message_id)

        message_id = self._count
        self._count += 1

        # msg_with_key = str(message_id)
        # msg_with_key += 'K'
        # msg_with_key += msg._data['text']
        # msg._data['text'] = msg_with_key

        sender_msg = Message(str(message_id), [msg._type, msg._data])
        ctx.send(sender_msg, self._receiver)
        # ctx.send(msg, self._receiver)

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

        # i = 0
        # key = ''
        # while msg._data['text'][i] != 'K':
        #     key += msg._data['text'][i]
        #     i += 1
        
        if msg._type not in self._unique_keys:
            self._unique_keys.append(msg._type)
            # msg._data['text'] = msg._data['text'][len(key) + 1 :]
            receiver_msg = Message(msg._data[0], msg._data[1])
            ctx.send_local(receiver_msg)
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
        self._finished = set()

    def on_local_message(self, msg: Message, ctx: Context):
        # receive message for delivery from local user
        message_id = str(uuid.uuid4())
        if len(message_id) > 50:
            message_id = message_id[:50]
        while message_id in self._finished:
            message_id = str(uuid.uuid4())
            if len(message_id) > 50:
                message_id = message_id[:50]
        self._not_finished[message_id] = msg
        sender_msg = Message(message_id, [msg._type, msg._data])
        ctx.send(sender_msg, self._receiver)
        ctx.set_timer(message_id, 3) 

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver here
        if msg._type not in self._finished:
            ctx.cancel_timer(msg._type)
            self._not_finished.pop(msg._type)
            self._finished.add(msg._type)

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        if timer_name in self._not_finished.keys():
            sender_msg = Message(timer_name, [self._not_finished[timer_name]._type, self._not_finished[timer_name]._data])
            ctx.send(sender_msg, self._receiver)
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
        receiver_msg = Message(msg._data[0], msg._data[1])
        ctx.send_local(receiver_msg)

        ans = Message(msg._type, msg._data)
        ctx.send(ans, sender) 

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass


# EXACTLY ONCE ---------------------------------------------------------------------------------------------------------

class ExactlyOnceSender(Process):
    def __init__(self, proc_id: str, receiver_id: str):
        self._id = proc_id
        self._receiver = receiver_id
        self._not_finished = {}
        self._finished = set()

    def on_local_message(self, msg: Message, ctx: Context):
        # receive message for delivery from local user
        message_id = str(uuid.uuid4())
        if len(message_id) > 50:
            message_id = message_id[:50]
        while message_id in self._finished:
            message_id = str(uuid.uuid4())
            if len(message_id) > 50:
                message_id = message_id[:50]
        self._not_finished[message_id] = msg
        sender_msg = Message(message_id, [msg._type, msg._data])
        ctx.send(sender_msg, self._receiver)
        ctx.set_timer(message_id, 3) 
        # pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver here
        if msg._type not in self._finished:
            ctx.cancel_timer(msg._type)
            self._not_finished.pop(msg._type)
            self._finished.add(msg._type)
        # pass

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        if timer_name in self._not_finished.keys():
            sender_msg = Message(timer_name, [self._not_finished[timer_name]._type, self._not_finished[timer_name]._data])
            ctx.send(sender_msg, self._receiver)
            ctx.set_timer(timer_name, 3)
        # pass

class ExactlyOnceReceiver(Process):
    def __init__(self, proc_id: str):
        self._id = proc_id
        self._unique_keys = set()

    def on_local_message(self, msg: Message, ctx: Context):
        # not used in this task
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver
        # deliver message to local user with ctx.send_local()
        if msg._type not in self._unique_keys:
            self._unique_keys.add(msg._type)
            receiver_msg = Message(msg._data[0], msg._data[1])
            ctx.send_local(receiver_msg)

            ans = Message(msg._type, msg._data)
            ctx.send(ans, sender) 
        else:
            ans = Message(msg._type, msg._data)
            ctx.send(ans, sender) 
        # pass

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass


# EXACTLY ONCE + ORDERED -----------------------------------------------------------------------------------------------

class ExactlyOnceOrderedSender(Process):
    def __init__(self, proc_id: str, receiver_id: str):
        self._id = proc_id
        self._receiver = receiver_id
        self._not_finished = {} 
        self._finished = set()
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
            self._finished.add(msg._type)
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
