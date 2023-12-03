
import hashlib
from dslabmp import Context, Message, Process
from typing import List
import ast
import random

class StorageNode(Process):
    def __init__(self, node_id: str, nodes: List[str]):
        self._id = node_id
        self._nodes = nodes
        self._data = {}
        self.quorum_put = {}
        self.quorum_get = {}
        self.get_value = None
        self.put_value = None
        self.delete_value = None
        self.safe_value = None
        self._versions = {}
        self._timers = {}

    def on_local_message(self, msg: Message, ctx: Context):
        # Get key value.
        # Request:
        #   GET {"key": "some key", "quorum": 1-3}
        # Response:
        #   GET_RESP {"key": "some key", "value": "value for this key"}
        #   GET_RESP {"key": "some key", "value": null} - if record for this key is not found
        if msg.type == 'GET':
            key = msg['key']
            print("[py] Key", key, "replicas:", get_key_replicas(key, len(self._nodes)))
            replicas = list(map(str, get_key_replicas(key, len(self._nodes))))
            if self._id in replicas:
                resp = Message('GET', {
                    'key': key,
                    'id_owner': self._id,
                    'quorum': msg['quorum'],
                    'context': self._versions.get(key, [])
                })
                name_key = str(msg['key']) + ":" + str(self._id)
                self.quorum_get[name_key] = [0]

                for replica in replicas:
                    ctx.send(resp, replica)

                self.safe_value = key

                timer_name = 'quorum_read'
                timer_name += ":"
                timer_name += key
                timer_name += ":"
                timer_name += str(msg['quorum'])
                timer_name += ":"
                timer_name += str(resp['id_owner'])

                ctx.set_timer(timer_name, 1)
            else:
                resp = Message('GET_COORDINATOR', {
                    'key': key,
                    'quorum': msg['quorum'],
                    'id_owner': self._id
                })
                ctx.send(resp, replicas[0])

                timer_name = 'get_coordinator'
                timer_name += ":"
                timer_name += key
                timer_name += ":"
                timer_name += str(msg['quorum'])
                timer_name += ":"
                timer_name += str(resp['id_owner'])
                ctx.set_timer(timer_name, 1)
                self._timers[timer_name] = [int(replicas[0])]

        # Store (key, value) record
        # Request:
        #   PUT {"key": "some key", "value: "some value", "quorum": 1-3}
        # Response:
        #   PUT_RESP {"key": "some key", "value: "some value"}
        elif msg.type == 'PUT':
            key = msg['key']
            value = msg['value'].split(',')
            replicas = list(map(str, get_key_replicas(key, len(self._nodes))))

            if self._id in replicas:

                if not self._versions.get(key):
                    self._versions[key] = [0] * len(self._nodes)
                if msg['context']: 
                    update_version_vector(self._versions[key], ast.literal_eval(msg['context']), self._data,
                                          msg['key'], msg['value'], self._id, False)

                self._versions[key][int(self._id)] += 1
                resp = Message('PUT', {
                    'key': key,
                    'value': value,
                    'id_owner': self._id,
                    'quorum': msg['quorum'],
                    'context': self._versions.get(key).copy()
                })

                name_key = str(msg['key']) + ":" + str(self._id)
                self.quorum_put[name_key] = [0]
                for replica in replicas:
                    ctx.send(resp, replica)
                self._versions[key][int(self._id)] -= 1

                self.safe_value = (key, value)

                timer_name = 'quorum_write'
                timer_name += ":"
                timer_name += key
                timer_name += ":"
                timer_name += str(msg['quorum'])
                timer_name += ":"
                timer_name += str(resp['id_owner'])
                ctx.set_timer(timer_name, 1)
            else:
                resp = Message('PUT_COORDINATOR', {
                    'key': key,
                    'value': value,
                    'quorum': msg['quorum'],
                    'id_owner': self._id
                })
                ctx.send(resp, replicas[0])
                timer_name = 'put_coordinator'
                timer_name += ":"
                timer_name += key
                timer_name += ":"
                timer_name += str(msg['quorum'])
                timer_name += ":"
                timer_name += str(resp['id_owner'])
                ctx.set_timer(timer_name, 1)
                self._timers[timer_name] = [int(replicas[0]), value]



    def on_message(self, msg: Message, sender: str, ctx: Context):
        # Implement node-to-node communication using any message types
        # pass
        if msg.type == 'PUT':
            if msg['key'] not in self._data.keys():
                self._data[msg['key']] = msg['value']
                self._versions[msg['key']] = msg['context']
                self._versions[msg['key']][int(self._id)] = 1
            else:
                update_version_vector(self._versions[msg['key']], msg['context'], self._data, msg['key'],
                                      msg['value'], self._id, True)
            resp = Message('PUT_RETURN', {
                'key': msg['key'],
                'value': self._data[msg['key']],
                'id': self._id,
                'id_owner': msg['id_owner'],
                'quorum': msg['quorum'],
                'context': self._versions[msg['key']]
            })
            ctx.send(resp, sender)
        elif msg.type == 'GET':
            value = self._data.get(msg['key'])

            resp = Message('GET_RETURN', {
                'key': msg['key'],
                'value': value,
                'id': self._id,
                'id_owner': msg['id_owner'],
                'quorum': msg['quorum'],
                'context': self._versions.get(msg['key'], [0]*len(self._nodes))
            })
            ctx.send(resp, sender)
        elif msg.type == 'CANCEL':
            ctx.cancel_timer(msg['name'])
        elif msg.type == 'PUT_COORDINATOR':
            key = msg['key']
            ctx.send(Message('CANCEL', {'name': f"put_coordinator:{key}:{msg['quorum']}:{msg['id_owner']}"}), sender)
            value = msg['value']
            replicas = list(map(str, get_key_replicas(key, len(self._nodes))))
            if not self._versions.get(key):
                self._versions[key] = [0] * len(self._nodes)

            self._versions[key][int(self._id)] += 1
            resp = Message('PUT', {
                'key': key,
                'value': value,
                'id_owner': sender,
                'quorum': msg['quorum'],
                'context': self._versions.get(key).copy()
            })

            name_key = str(msg['key']) + ":" + str(msg['id_owner'])
            self.quorum_put[name_key] = [0]
            for replica in replicas:
                ctx.send(resp, replica)
            self._versions[key][int(self._id)] -= 1

            self.safe_value = [key, value]

            timer_name = 'quorum_write'
            timer_name += ":"
            timer_name += key
            timer_name += ":"
            timer_name += str(msg['quorum'])
            timer_name += ":"
            timer_name += str(msg['id_owner'])
            ctx.set_timer(timer_name, 1)
        elif msg.type == 'GET_COORDINATOR':
            key = msg['key']
            ctx.send(Message('CANCEL', {'name': f"get_coordinator:{key}:{msg['quorum']}:{msg['id_owner']}"}), sender)
            replicas = list(map(str, get_key_replicas(key, len(self._nodes))))
            resp = Message('GET', {
                'key': key,
                'id_owner': sender,
                'quorum': msg['quorum'],
                'context': self._versions.get(key, [])
            })
            name_key = str(msg['key']) + ":" + str(msg['id_owner'])
            self.quorum_get[name_key] = [0]

            for replica in replicas:
                ctx.send(resp, replica)

            self.safe_value =[key]

            timer_name = 'quorum_read'
            timer_name += ":"
            timer_name += key
            timer_name += ":"
            timer_name += str(msg['quorum'])
            timer_name += ":"
            timer_name += str(msg['id_owner'])
            ctx.set_timer(timer_name, 1)

        elif msg.type == 'PUT_CHECK':
            if msg['key'] not in self._data.keys():
                self._data[msg['key']] = msg['value']
                self._versions[msg['key']] = msg['context']
                self._versions[msg['key']][int(self._id)] = 1
            else:
                update_version_vector(self._versions[msg['key']], msg['context'], self._data, msg['key'],
                                      msg['value'], self._id, False)

                if sorted(self._data[msg['key']]) != sorted(msg['value']):
                    check = Message('PUT_CHECK', {
                        'key': msg['key'],
                        'value': self._data[msg['key']],
                        'context': self._versions[msg['key']],
                        'timername': None
                    })
                    ctx.send(check, sender)
            if msg['timername'] is not None:
                resp = Message('CANCEL_TIMER_WRITE', {
                    'timername': msg['timername']
                })
                ctx.send(resp, sender)
        elif msg.type == 'CANCEL_TIMER_WRITE':
            ctx.cancel_timer(msg['timername'])
            self.safe_value = None
        elif msg.type == 'PUT_RESP':
            ctx.send_local(msg)
        elif msg.type == 'GET_RESP':
            ctx.send_local(msg)
        elif msg.type == 'PUT_RETURN':
            name_key = str(msg['key']) + ":" + str(msg['id_owner'])
            if self.quorum_put[name_key][0] != -1:
                self.quorum_put[name_key].append(self._id)
                if self.put_value is None:
                    self.put_value = (msg['value'], msg['context'], msg['id_owner'])
                else:
                    cur_data = {}
                    cur_data[msg['key']] = self.put_value[0]
                    update_version_vector(self.put_value[1], msg['context'], cur_data, msg['key'],
                                      msg['value'], self._id, False)
                    self.put_value = (cur_data[msg['key']], self.put_value[1], self.put_value[2])

                    value, context = self.put_value[0], self.put_value[1]
                    if younger(context, msg['context']):
                         self.put_value = (msg['value'], msg['context'], msg['id_owner'])

                    if msg['context']:
                        for i in range(len(context)):
                            context[i] = max(context[i], msg['context'][i])
            else:
                check = Message('PUT_CHECK', {
                    'key': msg['key'],
                    'value': self._data[msg['key']],
                    'context': self._versions[msg['key']],
                    'timername': None
                })
                ctx.send(check, sender)

            if len(self.quorum_put[name_key]) - 1 >= int(msg['quorum']):
                
                
                values = []

                value = self.put_value[0]

                values = []
                if type(value) is str:
                    values = [value]
                elif value != None:
                    values = value
                
                if str(msg['key']).startswith("CART") or str(msg['key']).startswith("XCART"):
                    values = [",".join(list(values))]

                resp = Message('PUT_RESP', {
                    'key': msg['key'],
                    'values': values,
                    'context': str(self.put_value[1])
                })
                if self._id == self.put_value[2]:

                    ctx.send_local(resp)

                else:
                    ctx.send(resp, self.put_value[2])

                check = Message('PUT_CHECK', {
                    'key': msg['key'],
                    'value': self.put_value[0],
                    'context': self.put_value[1],
                    'timername': None
                })

                replicas = list(map(str, get_key_replicas(msg['key'], len(self._nodes))))
                # number = random.randint(1, len(replicas) - 1)
                # for i in range(1, len(self.quorum_put[name_key])):
                for i in range(len(self.quorum_put[name_key]) - 1):
                    # ctx.send(check, self.quorum_put[name_key][i])
                    number = random.randint(1, len(replicas) - 1)
                    ctx.send(check, replicas[number])

                self.quorum_put[name_key] = [-1]
                

                timer_name = 'quorum_write'
                timer_name += ":"
                timer_name += msg['key']
                timer_name += ":"
                timer_name += str(msg['quorum'])
                timer_name += ":"
                timer_name += str(self.put_value[2])
                ctx.cancel_timer(timer_name)

                self.put_value = None

        elif msg.type == 'GET_RETURN':
            name_key = str(msg['key']) + ":" + str(msg['id_owner'])
            if self.quorum_get[name_key][0] != -1:
                self.quorum_get[name_key].append(self._id)
                if self.get_value is None:
                    self.get_value = (msg['value'], msg['context'], msg['id_owner'])
                else:
                    cur_data = {}
                    cur_data[msg['key']] = self.get_value[0]
                    update_version_vector(self.get_value[1], msg['context'], cur_data, msg['key'],
                                      msg['value'], self._id, False)
                    self.get_value = (cur_data[msg['key']], self.get_value[1], self.get_value[2])

                    value, context = self.get_value[0], self.get_value[1]
                    if younger(context, msg['context']):
                        self.get_value = (msg['value'], msg['context'], msg['id_owner'])

                    if msg['context']:
                        for i in range(len(context)):
                            context[i] = max(context[i], msg['context'][i])
            # else:
            #     print(self._data[msg['key']])
            #     check = Message('PUT_CHECK', {
            #         'key': msg['key'],
            #         'value': self._data[msg['key']],
            #         'context': self._versions[msg['key']],
            #         'timername': None
            #     })
            #     ctx.send(check, sender)

            if len(self.quorum_get[name_key]) - 1 >= int(msg['quorum']):
                if self.get_value is None:
                    value = None
                    context = None
                else:
                    value = self.get_value[0]
                    context = self.get_value[1]
                

                values = []
                if type(value) is str:
                    values = [value]
                elif value != None:
                    values = value
            
                if str(msg['key']).startswith("CART") or str(msg['key']).startswith("XCART"):
                    values = [",".join(list(values))]

                resp = Message('GET_RESP', {
                    'key': msg['key'],
                    'values': values,
                    'context': str(context)
                })
                
                if self._id == self.get_value[2]:
                    ctx.send_local(resp)
                else:
                    ctx.send(resp, self.get_value[2])

                if value is not None:
                    check = Message('PUT_CHECK', {
                        'key': msg['key'],
                        'value': value,
                        'context': context,
                        'timername': None
                    })

                    replicas = list(map(str, get_key_replicas(msg['key'], len(self._nodes))))

                    for replica in replicas:
                        ctx.send(check, replica)

                self.quorum_get[name_key] = [-1]

                timer_name = 'quorum_read'
                timer_name += ":"
                timer_name += msg['key']
                timer_name += ":"
                timer_name += str(msg['quorum'])
                timer_name += ":"
                timer_name += str(self.get_value[2])
                ctx.cancel_timer(timer_name)

                self.get_value = None
        elif msg.type == 'PUT_RESERV':
            if msg['key'] not in self._data.keys():
                self._data[msg['key']] = msg['value']
                self._timestamp_key[msg['key']] = msg['timestamp']
            else:
                if self._timestamp_key[msg['key']] < msg['timestamp']:
                    self._data[msg['key']] = msg['value']
                    self._timestamp_key[msg['key']] = msg['timestamp']
                elif self._timestamp_key[msg['key']] == msg['timestamp']:
                    if self._data[msg['key']] < msg['value']:
                        self._data[msg['key']] = msg['value']
                        self._timestamp_key[msg['key']] = msg['timestamp']
            resp = Message('PUT_RESP_RESERV', {
                'key': msg['key'],
                'value': self._data[msg['key']],
                'id': self._id,
                'timestamp': self._timestamp_key[msg['key']]
            })
            ctx.send(resp, msg['id_owner'])

            replicas = list(map(str, get_key_replicas(msg['key'], len(self._nodes))))
            for replica in replicas:
                timername = 'put_alive'
                timername += ':'
                timername += msg['key']
                timername += ':'
                timername += replica
                timername += ':'
                timername += msg['id_owner']
                ctx.set_timer(timername, 1)

            resp2 = Message('CANCEL_TIMER_WRITE', {
                'timername': msg['timername']
            })
            ctx.send(resp2, msg['id_owner'])
        elif msg.type == 'PUT_RESP_RESERV':
            resp = Message('PUT_RESP', {
                'key': msg['key'],
                'value': msg['value'],
            })
            ctx.send_local(resp)
        elif msg.type == 'GET_RESERV':
            value = self._data.get(msg['key'])
            timestamp = self._timestamp_key.get(msg['key'])
            resp = Message('GET_RESP_RESERV', {
                'key': msg['key'],
                'value': value,
                'id': self._id,
                'timestamp': timestamp,
                'timername': msg['timername']
            })
            ctx.send(resp, msg['id_owner'])
        elif msg.type == 'GET_RESP_RESERV':
            ctx.cancel_timer(msg['timername'])
            resp = Message('GET_RESP', {
                'key': msg['key'],
                'value': msg['value'],
            })
            ctx.send_local(resp)

    def on_timer(self, timer_name: str, ctx: Context):
        name, key, quorum, id_owner = timer_name.split(":")
        if name == 'quorum_write':
            name_key = key + ":" + id_owner
            if self.quorum_put[name_key][0] != -1 and len(self.quorum_put[name_key]) - 1 < int(quorum):
                self.quorum_put[name_key] = [-1]

                if self.put_value is None:
                    replicas = list(map(str, get_key_replicas(key, len(self._nodes))))
                    reserv_replica = 0
                    for i in range(len(self._nodes) - 1, -1, -1):
                        if self._nodes[i] == replicas[2]:
                            if i == len(self._nodes) - 1:
                                reserv_replica = self._nodes[0]
                            else:
                                reserv_replica = self._nodes[i + 1]
                            break
                    key, value = self.safe_value[0], self.safe_value[1]
                    resp = Message('PUT_RESERV', {
                        'key': key,
                        'value': value,
                        'id_owner': id_owner,
                        'timername': timer_name
                    })
                    ctx.send(resp, reserv_replica)
                else:
                    ctx.cancel_timer(timer_name)

                    values = []
                    if type(self.put_value[0]) is str:
                        values = [self.put_value[0]]
                    elif self.put_value[0] != None:
                        values = self.put_value[0]
                    
                    if str(key).startswith("CART") or str(key).startswith("XCART"):
                        values = [",".join(list(values))]

                    resp = Message('PUT_RESP', {
                        'key': key,
                        'values': values,
                        'context': str(self.put_value[1])
                    })

                    ctx.send(resp, id_owner)

                    replicas = list(map(str, get_key_replicas(key, len(self._nodes))))
                    for replica in replicas:
                        timername = 'put_alive'
                        timername += ':'
                        timername += key
                        timername += ':'
                        timername += replica
                        timername += ':'
                        timername += id_owner
                        ctx.set_timer(timername, 1)

                    self.put_value = None
            else:
                ctx.cancel_timer(timer_name)
        elif name == 'put_alive':
            check = Message('PUT_CHECK', {
                'key': key,
                'value': self._data.get(key),
                'context':self._versions.get(key),
                'timername': timer_name
            })
            ctx.send(check, quorum)
            ctx.set_timer(timer_name, 1)
        elif name == 'quorum_read':
            name_key = key + ":" + id_owner
            if self.quorum_get[name_key][0] != -1 and len(self.quorum_get[name_key]) - 1 < int(quorum):
                self.quorum_get[name_key] = [-1]

                if self.get_value is None:
                    replicas = list(map(str, get_key_replicas(key, len(self._nodes))))
                    reserv_replica = 0
                    for i in range(len(self._nodes) - 1, -1, -1):
                        if self._nodes[i] == replicas[2]:
                            if i == len(self._nodes) - 1:
                                reserv_replica = self._nodes[0]
                            else:
                                reserv_replica = self._nodes[i + 1]
                            break

                    resp = Message('GET_RESERV', {
                        'key': key,
                        'id_owner': self._id,
                        'timername': timer_name
                    })
                    ctx.send(resp, reserv_replica)
                else:
                    ctx.cancel_timer(timer_name)

                    values = []
                    if type(self.get_value[0]) is str:
                        values = [self.get_value[0]]
                    elif self.get_value[0] != None:
                        values = self.get_value[0]

                    if str(key).startswith("CART") or str(key).startswith("XCART"):
                        values = [",".join(list(values))]

                    resp = Message('GET_RESP', {
                        'key': key,
                        'values': values,
                        'context': str(self.get_value[1])
                    })
                    ctx.send(resp, id_owner)
                    self.get_value = None

            else:
                ctx.cancel_timer(timer_name)
        elif name == 'get_coordinator':
            next_replica = get_next_replica(self._timers[timer_name][0], len(self._nodes))
            resp = Message('GET_COORDINATOR', {
                'key': key,
                'quorum': quorum,
                'id_owner' : id_owner
            })
            ctx.send(resp, str(next_replica))
            self._timers[timer_name] = [next_replica]
            ctx.set_timer(timer_name, 1)
        elif name == 'put_coordinator':
            next_replica = get_next_replica(self._timers[timer_name][0], len(self._nodes))
            resp = Message('PUT_COORDINATOR', {
                'key': key,
                'value': self._timers[timer_name][1],
                'quorum': quorum,
                'id_owner' : id_owner
            })
            ctx.send(resp, str(next_replica))
            self._timers[timer_name] = [next_replica, self._timers[timer_name][1]]
            ctx.set_timer(timer_name, 1)


def get_key_replicas(key: str, node_count: int):
    replicas = []
    key_hash = int.from_bytes(hashlib.md5(key.encode('utf8')).digest(), 'little', signed=False)
    cur = key_hash % node_count
    for _ in range(3):
        replicas.append(cur)
        cur = get_next_replica(cur, node_count)
    return replicas


def get_next_replica(i, node_count: int):
    return (i + 1) % node_count

def younger(version: list, other_version: list):
    if version and other_version:
        for i in range(len(version)):
            if other_version[i] < version[i]:
                return False
    return True

def update_version_vector(my_vector:list, context:list, data:dict, key, value, id:str, flag_put):
    flag_younger = younger(my_vector, context)
    flag_older = younger(context, my_vector)

    if flag_younger and context != my_vector:
        data[key] = value
        if flag_put:
            my_vector[int(id)] += 1
    if not flag_younger and not flag_older:
        values = set()
        if type(value) is list:
            for el in value:
                values.add(el)
        else:
            values.add(value)
        if type(data[key]) is list:
            for el in data[key]:
                values.add(el)
        else:
            values.add(data[key])
        data[key] = list(values)
    for i in range(len(my_vector)):
        my_vector[i] = max(my_vector[i], context[i])
