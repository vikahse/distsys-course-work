from dslabmp import Context, Message, Process
import random


class GroupMember(Process):
    def __init__(self, proc_id: str):
        self._id = proc_id
        self._group = []
        self._delivered = []
        self._done = []
        self._leaved = []
        # self._lost = []
 
    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type == 'JOIN':
            seed = msg['seed']
            if seed == self._id:
                # print(seed)
                # create new empty group and add process to it
                self._group = []
                self._group.append(self._id)
            else:
                # join existing group

                timer_name = 'SUSPECTED'
                timer_name += ':'
                timer_name += str(seed)
                ctx.set_timer(timer_name, 3) 

                msg_new = Message('JOIN', {'id': self._id, 'group' : self._group})
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
        elif msg.type == 'DELIVER':
            msg_new = Message('JOIN', {'id': msg._data['id'], 'group' : self._group})
            for member in self._group:
                ctx.send(msg_new, member)


    def on_message(self, msg: Message, sender: str, ctx: Context):
        # Implement communication between processes using any message types
        if msg._type == 'JOIN':
            # print(self._id, msg._data['id'], self._group)

            # deliver_msg = Message('DELIVER', {
            #     'id': msg._data['id'],
            #     'group' : self._group
            # })

            # if msg._data['id'] in list(self._lost):
            #     self._lost.remove( msg._data['id'])

            if msg._data['id'] not in self._group:
                self._group.append(msg._data['id'])
                # if msg._data['id'] != self._id:
                timer_name = 'SUSPECTED'
                timer_name += ':'
                timer_name += str(msg._data['id'])
                ctx.set_timer(timer_name, 3) 

                if msg._data['id'] in self._leaved:
                    self._leaved.remove(msg._data['id'])

            deliver_msg = Message('DELIVER', {
                'id': msg._data['id'],
                'group' : self._group
            })

            if msg._data['id'] not in self._delivered:
                self._delivered.append(msg._data['id'])
                if self._id != sender:
                    self.on_local_message(deliver_msg, ctx)
            
            for el in msg._data['group']:
                if el not in self._group:
                    self._group.append(el)
                    if el not in self._delivered:
                        deliver_msg_new = Message('DELIVER', {
                                'id': el,
                                'group' : self._group
                            })
                        self._delivered.append(el)
                        if self._id != sender:
                            self.on_local_message(deliver_msg_new, ctx)
                    
                    if el in self._leaved:
                        self._leaved.remove(el)

                    # # if el != self._id:
                    timer_name = 'SUSPECTED'
                    timer_name += ':'
                    timer_name += str(el)
                    ctx.set_timer(timer_name, 3) 

        elif msg._type == 'LEAVE':
            if msg._data['member'] in self._done:
                self._done.remove(msg._data['member'])
            if self._id == msg._data['member']:
                self._id = []
            if len(self._group) != 0:
                if msg._data['member'] in self._group:
                    self._group.remove(msg._data['member'])
                    self._delivered.remove(msg._data['member'])
                    self._leaved.append(msg._data['member'])
        elif msg._type == 'LEAVE2':
            if msg._data['member'] in self._done:
                self._done.remove(msg._data['member'])
            if self._id == msg._data['member']:
                self._id = []
            if len(self._group) != 0:
                if msg._data['member'] in self._group:
                    self._group.remove(msg._data['member'])
                    self._delivered.remove(msg._data['member'])
                    # if msg._data['member'] not in self._lost:
                    #     self._lost.append(msg._data['member'])
                    # self._leaved.append(msg._data['member'])
        elif msg._type == 'ALIVE':
                # print('ALIVE', self._id, sender, self._group, self._done, self._delivered)
                alive_msg = Message('DONE', {
                    'id': self._id,
                    'timer_name' : msg._data['timer_name']
                })
                ctx.send(alive_msg, sender)
        elif msg._type == 'DONE':
            # ctx.cancel_timer(msg._data['timer_name'])
            # print('DONE', self._id)
            if msg._data['id'] not in self._group and msg._data['id'] not in self._leaved:
                # print('JOIN AGAIN', self._id)
                msg_new = Message('JOIN', {'id': msg._data['id'], 'group' : self._group})
                ctx.send(msg_new, self._id)

                # timer_name = 'SUSPECTED'
                # timer_name += ':'
                # timer_name += str(msg._data['id'])
                # ctx.set_timer(timer_name, 3) 

            else:
                self._done.append(msg._data['id'])
        elif msg._type == 'CHECK':
            for member in msg._data['members']:
                if member not in self._group and member not in self._leaved:
                    self._group.append(member)
                    if member not in self._delivered:
                        deliver_msg_new = Message('DELIVER', {
                                'id': member,
                            })
                        self._delivered.append(member)
                        if self._id != sender:
                            self.on_local_message(deliver_msg_new, ctx)

                    timer_name = 'SUSPECTED'
                    timer_name += ':'
                    timer_name += str(member)
                    ctx.set_timer(timer_name, 3) 


    def on_timer(self, timer_name: str, ctx: Context):
        # print(timer_name, self._id, self._done, self._group)
        if timer_name.split(':')[0] == 'SUSPECTED':
            # print('SUSPECTED', timer_name.split(':')[1])
            # if timer_name.split(':')[1] in self._lost:
            #     self._lost.remove(timer_name.split(':')[1])
            msg = Message('ALIVE', {'timer_name': timer_name, 'id' : timer_name.split(':')[1]}) 
            ctx.send(msg, timer_name.split(':')[1])
            new_timer_name = 'WAIT'
            new_timer_name += ':'
            new_timer_name += timer_name.split(':')[1]
            ctx.set_timer(new_timer_name, 3)
            ctx.cancel_timer(timer_name)
        elif timer_name.split(':')[0] == 'WAIT':
            if timer_name.split(':')[1] not in self._done:
                if timer_name.split(':')[1] not in self._leaved:
                    msg = Message('LEAVE2', {'member': timer_name.split(':')[1]}) 
                    # print('LEAVE2', timer_name.split(':')[1], self._id)
                    for el in self._group:
                        ctx.send(msg, el)
                # ctx.cancel_timer(timer_name)
                    # new_timer_name = 'SUSPECTED'
                    # new_timer_name += ':'
                    # new_timer_name += timer_name.split(':')[1]
                    # ctx.set_timer(new_timer_name, 3)
            else:
                self._done.remove(timer_name.split(':')[1])
                new_timer_name = 'SUSPECTED'
                new_timer_name += ':'
                new_timer_name += timer_name.split(':')[1]
                msg = Message('CHECK', {'members': self._group}) 
                # print('CHECK', self._group)
                for member in self._group:
                    if member != self._id:
                        ctx.send(msg, member)
                # for member in self._lost:
                #     print(self._id, self._lost)
                #     new_timer_name = 'SUSPECTED'
                #     new_timer_name += ':'
                #     new_timer_name += str(member)
                #     ctx.set_timer(new_timer_name, 3)

                ctx.set_timer(new_timer_name, 3)
                # ctx.cancel_timer(timer_name)