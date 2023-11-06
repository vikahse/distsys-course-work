from dslabmp import Context, Message, Process
# import random


class GroupMember(Process):
    def __init__(self, proc_id: str):
        self._id = proc_id
        self._group = []
        self._delivered = []
        self._done = []
        self._leaved = []

    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type == 'JOIN':
            seed = msg['seed']
            if seed == self._id:
                # create new empty group and add process to it
                self._group = []
                self._group.append(self._id)
            else:
                # join existing group
                if self._id not in self._group:
                    self._group.append(self._id)

                timer_name = 'SUSPECTED'
                timer_name += ':'
                timer_name += str(seed)
                ctx.set_timer(timer_name, 3)

                msg_new = Message(
                    'JOIN', {'id': self._id, 'group': self._group})
                ctx.send(msg_new, seed)
        elif msg.type == 'LEAVE':
            # Remove process from the group
            msg_new = Message('LEAVE', {'member': self._id})
            for el in self._group:
                ctx.send(msg_new, el)
        elif msg.type == 'GET_MEMBERS':
            # Get a list of group members
            # - return the list of all known alive processes in MEMBERS message
            msg_new = Message('MEMBERS', {'members': self._group})
            ctx.send_local(msg_new)
        # elif msg.type == 'DELIVER':
        #     msg_new = Message(
        #         'JOIN', {'id': msg._data['id'], 'group': self._group})
        #     for member in self._group:
        #         ctx.send(msg_new, member)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # Implement communication between processes using any message types
        if msg._type == 'JOIN':
            if msg._data['id'] not in self._group:
                self._group.append(msg._data['id'])
                if self._id != msg._data['id']:
                    timer_name = 'SUSPECTED'
                    timer_name += ':'
                    timer_name += str(msg._data['id'])
                    ctx.set_timer(timer_name, 3)

            if msg._data['id'] in self._leaved:
                self._leaved.remove(msg._data['id'])

            for el in msg._data['group']:
                if el not in self._group:
                    self._group.append(el)

                    if el in self._leaved:
                        self._leaved.remove(el)

                    if self._id != el:
                        timer_name = 'SUSPECTED'
                        timer_name += ':'
                        timer_name += str(el)
                        ctx.set_timer(timer_name, 3)

            deliver_msg = Message('JOIN', {
                'id': msg._data['id'],
                'group': self._group
            })

            for member in self._group:
                if member not in self._delivered:
                    self._delivered.append(member)
                    if member != self._id:
                        ctx.send(deliver_msg, member)
        elif msg._type == 'LEAVE':
            if msg._data['member'] in self._done:
                self._done.remove(msg._data['member'])
            if len(self._group) != 0:
                if msg._data['member'] in self._group:
                    self._group.remove(msg._data['member'])
                    self._leaved.append(msg._data['member'])
        elif msg._type == 'LEAVE2':
            if msg._data['member'] in self._done:
                self._done.remove(msg._data['member'])
            if len(self._group) != 0:
                if msg._data['member'] in self._group:
                    self._group.remove(msg._data['member'])
        elif msg._type == 'ALIVE':
            alive_msg = Message('DONE', {
                'id': self._id,
                'timer_name': msg._data['timer_name'],
                'group': self._group,
            })
            ctx.send(alive_msg, sender)
        elif msg._type == 'DONE':
            if msg._data['id'] not in self._group and msg._data['id'] not in self._leaved:
                msg_new = Message(
                    'JOIN', {'id': msg._data['id'], 'group': self._group})
                for member in self._group:
                    ctx.send(msg_new, member)
                if self._id != msg._data['id']:
                    timer_name = 'SUSPECTED'
                    timer_name += ':'
                    timer_name += str(msg._data['id'])
                    ctx.set_timer(timer_name, 3)
            self._done.append(msg._data['id'])
        elif msg._type == 'CHECK':
            for member in msg._data['members']:
                if member not in self._group and member not in self._leaved:
                    self._group.append(member)
                    if member != self._id:
                        timer_name = 'SUSPECTED'
                        timer_name += ':'
                        timer_name += str(member)
                        ctx.set_timer(timer_name, 3)

    def on_timer(self, timer_name: str, ctx: Context):
        if timer_name.split(':')[0] == 'SUSPECTED':
            if timer_name.split(':')[1] in self._done:
                self._done.remove(timer_name.split(':')[1])

            msg = Message(
                'ALIVE', {'timer_name': timer_name, 'id': timer_name.split(':')[1]})
            ctx.send(msg, timer_name.split(':')[1])

            if self._id != timer_name.split(':')[1]:
                new_timer_name = 'WAIT'
                new_timer_name += ':'
                new_timer_name += timer_name.split(':')[1]
                ctx.set_timer(new_timer_name, 3)

            ctx.cancel_timer(timer_name)
        elif timer_name.split(':')[0] == 'WAIT':
            if timer_name.split(':')[1] not in self._done:
                if timer_name.split(':')[1] not in self._leaved:
                    msg = Message(
                        'LEAVE2', {'member': timer_name.split(':')[1]})
                    for el in self._group:
                        if el != timer_name.split(':')[1]:
                            ctx.send(msg, el)
                    new_timer_name = 'SUSPECTED'
                    new_timer_name += ':'
                    new_timer_name += str(timer_name.split(':')[1])
                    ctx.set_timer(new_timer_name, 3)
            elif timer_name.split(':')[1] in self._done and timer_name.split(':')[1] in self._leaved:
                self._done.remove(timer_name.split(':')[1])
                msg = Message('LEAVE', {'member': timer_name.split(':')[1]})
                for el in self._group:
                    if el != timer_name.split(':')[1]:
                        ctx.send(msg, el)
            else:
                self._done.remove(timer_name.split(':')[1])
                new_timer_name = 'SUSPECTED'
                new_timer_name += ':'
                new_timer_name += timer_name.split(':')[1]
                msg = Message('CHECK', {'members': self._group})
                for member in self._group:
                    if member != self._id:
                        ctx.send(msg, member)
                if self._id != timer_name.split(':')[1]:
                    ctx.set_timer(new_timer_name, 3)
            ctx.cancel_timer(timer_name)
