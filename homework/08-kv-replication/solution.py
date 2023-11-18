import hashlib
from dslabmp import Context, Message, Process
from typing import List


class StorageNode(Process):
    def __init__(self, node_id: str, nodes: List[str]):
        self._id = node_id
        self._nodes = nodes
        self._data = {}
        self._timestamp_key = {}
        self.quorum_put = {}
        self.quorum_get = {}
        self.quorum_delete = {}
        self.get_value = None
        self.put_value = None
        self.delete_value = None
        self.safe_value = None

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
            replicas = get_key_replicas(key, len(self._nodes))
            resp = Message('GET', {
                'key': key,
                'id_owner' : self._id,
                'quorum' : msg['quorum'],
                'timestamp' : ctx.time()
            })
            self.quorum_get[msg['key']] = 0

            for replica in replicas:
                ctx.send(resp, replica)


            self.safe_value = (key, resp['timestamp'])

            timer_name = 'quorum_read'
            timer_name += ":"
            timer_name += key
            timer_name += ":"
            timer_name += str(msg['quorum'])
            ctx.set_timer(timer_name, 1)

        # Store (key, value) record
        # Request:
        #   PUT {"key": "some key", "value: "some value", "quorum": 1-3}
        # Response:
        #   PUT_RESP {"key": "some key", "value: "some value"}
        elif msg.type == 'PUT':
            key = msg['key']
            value = msg['value']
            replicas = get_key_replicas(key, len(self._nodes))
            timestamp = ctx.time()
            resp = Message('PUT', {
                'key': key,
                'value': value,
                'id_owner' : self._id,
                'quorum' : msg['quorum'],
                'timestamp' : timestamp
            })
        
            self.quorum_put[msg['key']] = 0
            for replica in replicas:
                ctx.send(resp, replica)
            
            self.safe_value =(key, value, timestamp)

            timer_name = 'quorum_write'
            timer_name += ":"
            timer_name += key
            timer_name += ":"
            timer_name += str(msg['quorum'])
            ctx.set_timer(timer_name, 1)


        # Delete value for the key
        # Request:
        #   DELETE {"key": "some key", "quorum": 1-3}
        # Response:
        #   DELETE_RESP {"key": "some key", "value": "some value"}
        elif msg.type == 'DELETE':
            key = msg['key']
            replicas = get_key_replicas(key, len(self._nodes))
            resp = Message('DELETE', {
                'key': key,
                'id_owner' : self._id,
                'quorum' : msg['quorum'],
                'timestamp' : ctx.time()
            })
            self.quorum_delete[msg['key']] = 0
            for replica in replicas:
                ctx.send(resp, replica)


    def on_message(self, msg: Message, sender: str, ctx: Context):
        # Implement node-to-node communication using any message types
        # pass
        if msg.type == 'PUT':
            if msg['key'] not in self._data.keys():
                self._data[msg['key']] = msg['value']
                self._timestamp_key[msg['key']] = msg['timestamp']
            else:
                if self._timestamp_key[msg['key']] < msg['timestamp']:
                    self._data[msg['key']] = msg['value']
                    self._timestamp_key[msg['key']] = msg['timestamp']
                elif self._timestamp_key[msg['key']] == msg['timestamp']:
                    if self._data[msg['key']] <  msg['value']:
                        self._data[msg['key']] = msg['value']
                        self._timestamp_key[msg['key']] = msg['timestamp']
            resp = Message('PUT_RESP', {
                'key': msg['key'],
                'value': self._data[msg['key']],
                'id' : self._id,
                'quorum' : msg['quorum'],
                'timestamp' : self._timestamp_key[msg['key']]
            })
            ctx.send(resp, msg['id_owner'])
        elif msg.type == 'GET':
            value = self._data.get(msg['key'])
            timestamp = self._timestamp_key.get(msg['key'])

            resp = Message('GET_RESP', {
                'key': msg['key'],
                'value': value,
                'id' : self._id,
                'quorum' : msg['quorum'],
                'timestamp' : timestamp
            })
            ctx.send(resp, msg['id_owner'])
        elif msg.type == 'DELETE':
            value = self._data.pop(msg['key'], None)
            self._data[msg['key']] = None
            timestamp = self._timestamp_key[msg['key']]
            self._timestamp_key[msg['key']] = msg['timestamp']
            resp = Message('DELETE_RESP', {
                'key': msg['key'],
                'value': value,
                'id' : self._id,
                'quorum' : msg['quorum'],
                'timestamp' : timestamp,
                'timestamp_del' : msg['timestamp']
            })
            ctx.send(resp, msg['id_owner'])
        elif msg.type == 'PUT_CHECK':
            if msg['key'] not in self._data.keys():
                self._data[msg['key']] = msg['value']
                self._timestamp_key[msg['key']] = msg['timestamp']
            else:
                if msg['timestamp'] is not None:
                    if self._timestamp_key[msg['key']] < msg['timestamp']:
                        self._data[msg['key']] = msg['value']
                        self._timestamp_key[msg['key']] = msg['timestamp']
                    elif self._timestamp_key[msg['key']] == msg['timestamp']:
                        if self._data[msg['key']] is not None:
                            if self._data[msg['key']] <  msg['value']:
                                self._data[msg['key']] = msg['value']
                                self._timestamp_key[msg['key']] = msg['timestamp']   
            if msg['timername'] is not None:
                resp = Message('CANCEL_TIMER_WRITE', {
                    'timername' : msg['timername']
                }) 
                ctx.send(resp, sender)      
        elif msg.type == 'CANCEL_TIMER_WRITE':
            ctx.cancel_timer(msg['timername'])
            self.safe_value = None
        elif msg.type == 'PUT_RESP':
            if self.quorum_put[msg['key']] != -1:
                self.quorum_put[msg['key']] += 1
                if self.put_value is None:
                    self.put_value = (msg['value'], msg['timestamp'])
                else:
                    value, timestamp = self.put_value[0], self.put_value[1]
                    if timestamp < msg['timestamp']:
                        self.put_value[0] = msg['value']
                        self.put_value[1] = msg['timestamp']
            
            if  self.quorum_put[msg['key']] >= msg['quorum']:
                resp = Message('PUT_RESP', {
                    'key': msg['key'],
                    'value': self.put_value[0],
                })
                ctx.send_local(resp)

                check =  Message('PUT_CHECK', {
                    'key': msg['key'],
                    'value': self.put_value[0],
                    'timestamp' : self.put_value[1],
                    'timername' : None
                })

                replicas = get_key_replicas(msg['key'], len(self._nodes))
                for replica in replicas:
                    ctx.send(check, replica)

                self.quorum_put[msg['key']] = -1
                self.put_value = None
        elif msg.type == 'GET_RESP':
            if self.quorum_get[msg['key']] != -1:
                self.quorum_get[msg['key']] += 1
                if self.get_value is None:
                    self.get_value = (msg['value'], msg['timestamp'])
                else:
                    value, timestamp = self.get_value[0], self.get_value[1]
                    if timestamp is not None and msg['timestamp'] is not None:
                        if timestamp < msg['timestamp']:
                            self.get_value = (msg['value'], msg['timestamp'])
                    elif timestamp is None and msg['timestamp'] is not None:
                            self.get_value = (msg['value'], msg['timestamp'])
            if self.quorum_get[msg['key']] >= msg['quorum']:
                if self.get_value is None:
                    value = None
                    timestamp = None
                else:
                    value = self.get_value[0]
                    timestamp =  self.get_value[1]
                resp = Message('GET_RESP', {
                    'key': msg['key'],
                    'value': value,
                })
                self.get_value = None
                ctx.send_local(resp)

                if value is not None:
                    check =  Message('PUT_CHECK', {
                        'key': msg['key'],
                        'value': value,
                        'timestamp' : timestamp,
                        'timername' : None
                    })

                    replicas = get_key_replicas(msg['key'], len(self._nodes))
                    for replica in replicas:
                        ctx.send(check, replica)

                self.quorum_get[msg['key']] = -1
        elif msg.type == 'DELETE_RESP':
            if self.quorum_delete[msg['key']] != -1:
                self.quorum_delete[msg['key']] += 1
                if self.delete_value is None:
                    self.delete_value = (msg['value'], msg['timestamp'])
                else:
                    value, timestamp = self.delete_value[0], self.delete_value[1]
                    if timestamp < msg['timestamp']:
                        self.delete_value = (msg['value'], msg['timestamp'])

            if self.quorum_delete[msg['key']] >= msg['quorum']:
                resp = Message('DELETE_RESP', {
                    'key': msg['key'],
                    'value': self.delete_value[0],
                })
                ctx.send_local(resp)
                self.quorum_delete[msg['key']] = -1

                check =  Message('PUT_CHECK', {
                        'key': msg['key'],
                        'value': self.delete_value[0],
                        'timestamp' : msg['timestamp_del'],
                        'timername' : None
                    })

                replicas = get_key_replicas(msg['key'], len(self._nodes))
                for replica in replicas:
                    ctx.send(check, replica)

                self.delete_value = None
        elif msg.type == 'PUT_RESERV':
            if msg['key'] not in self._data.keys():
                self._data[msg['key']] = msg['value']
                self._timestamp_key[msg['key']] = msg['timestamp']
            else:
                if self._timestamp_key[msg['key']] < msg['timestamp']:
                    self._data[msg['key']] = msg['value']
                    self._timestamp_key[msg['key']] = msg['timestamp']
                elif self._timestamp_key[msg['key']] == msg['timestamp']:
                    if self._data[msg['key']] <  msg['value']:
                        self._data[msg['key']] = msg['value']
                        self._timestamp_key[msg['key']] = msg['timestamp']
            resp = Message('PUT_RESP_RESERV', {
                'key': msg['key'],
                'value': self._data[msg['key']],
                'id' : self._id,
                'timestamp' : self._timestamp_key[msg['key']]
            })
            ctx.send(resp, msg['id_owner'])

            replicas = get_key_replicas(msg['key'], len(self._nodes))
            for replica in replicas:
                timername = 'put_alive'
                timername += ':'
                timername += msg['key']
                timername += ':'
                timername += replica
                ctx.set_timer(timername, 1)
            
            resp2 = Message('CANCEL_TIMER_WRITE', {
                'timername' : msg['timername']
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
                'id' : self._id,
                'timestamp' : timestamp,
                'timername' : msg['timername']
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
        name, key, quorum = timer_name.split(":")
        if name == 'quorum_write':
            print(self.safe_value)
            if self.quorum_put[key] != -1 and self.quorum_put[key] < int(quorum):
                print(1)
                self.quorum_put[key] = -1

                if self.put_value is None:
                    replicas = get_key_replicas(key, len(self._nodes))
                    reserv_replica = 0
                    for i in range(len(self._nodes) - 1, -1, -1):
                        if self._nodes[i] == replicas[2]:
                            if i == len(self._nodes) - 1:
                                reserv_replica = self._nodes[0]
                            else:
                                reserv_replica = self._nodes[i + 1]
                            break
                    key, value, timestamp = self.safe_value[0], self.safe_value[1], self.safe_value[2] 
                    resp =  Message('PUT_RESERV', {
                                'key': key,
                                'value': value,
                                'timestamp' : timestamp,
                                'id_owner' : self._id,
                                'timername' : timer_name
                            })
                    ctx.send(resp, reserv_replica)
                else:
                    ctx.cancel_timer(timer_name)
                    resp = Message('PUT_RESP', {
                            'key':key,
                            'value': self.put_value[0],
                    })
                    ctx.send_local(resp)

                    replicas = get_key_replicas(key, len(self._nodes))
                    for replica in replicas:
                        timername = 'put_alive'
                        timername += ':'
                        timername += key
                        timername += ':'
                        timername += replica
                        ctx.set_timer(timername, 1)

                    self.put_value = None
            else:
                ctx.cancel_timer(timer_name)
        elif name == 'put_alive':
            check =  Message('PUT_CHECK', {
                        'key': key,
                        'value': self._data.get(key),
                        'timestamp' : self._timestamp_key.get(key),
                        'timername' : timer_name
            })
            ctx.send(check, quorum)
            ctx.set_timer(timer_name, 1)
        elif name == 'quorum_read':
            if self.quorum_get[key] != -1 and self.quorum_get[key] < int(quorum):
                self.quorum_get[key] = -1

                if self.get_value is None:
                    replicas = get_key_replicas(key, len(self._nodes))
                    reserv_replica = 0
                    for i in range(len(self._nodes) - 1, -1, -1):
                        if self._nodes[i] == replicas[2]:
                            if i == len(self._nodes) - 1:
                                reserv_replica = self._nodes[0]
                            else:
                                reserv_replica = self._nodes[i + 1]
                            break
                    
                    resp =  Message('GET_RESERV', {
                                'key': key,
                                'id_owner' : self._id,
                                'timername' : timer_name
                            })
                    ctx.send(resp, reserv_replica)
                else:
                    ctx.cancel_timer(timer_name)
                    resp = Message('GET_RESP', {
                            'key':key,
                            'value': self.get_value[0],
                    })
                    ctx.send_local(resp)
                    self.get_value = None
                
            else:
                ctx.cancel_timer(timer_name)


def get_key_replicas(key: str, node_count: int):
    replicas = []
    key_hash = int.from_bytes(hashlib.md5(key.encode('utf8')).digest(), 'little', signed=False)
    cur = key_hash % node_count
    for _ in range(3):
        replicas.append(str(cur))
        cur = get_next_replica(cur, node_count)
    return replicas


def get_next_replica(i, node_count: int):
    return (i + 1) % node_count
