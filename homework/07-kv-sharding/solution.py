from dslabmp import Context, Message, Process
from typing import List
import hashlib
from collections import defaultdict
import operator
from functools import reduce
import base64


class StorageNode(Process):
    def __init__(self, node_id: str, nodes: List[str]):
        self._id = node_id
        self._nodes = set(nodes)
        self._data = {}
        self._next_node = '0'
        self._prev_node = '0'

        cur_nodes = [int(item) for item in self._nodes]
        cur_nodes = sorted(cur_nodes)
        for i in range(len(cur_nodes)):
            if cur_nodes[i] == int(self._id):
                if i != 0:
                    self._prev_node = str(cur_nodes[i - 1])
                else:
                    self._prev_node = str(cur_nodes[-1])

                if i != len(cur_nodes) - 1:
                    self._next_node = str(cur_nodes[i + 1])
                    break
                else:
                    self._next_node = str(cur_nodes[0])
                    break

    def get_hash_value(self, key):
        hash_object = hashlib.sha256(key.encode())
        hash_value = int(hash_object.hexdigest(), 32)
        return hash_value % (len(self._nodes))

    def get_id_node(self, hash_value):
        min_difference = 999999999999
        get_node = self._id
        for node in self._nodes:
            difference = abs(int(node) - hash_value)
            if difference < min_difference:
                min_difference = difference
                get_node = node
        return get_node

    def on_local_message(self, msg: Message, ctx: Context):
        # Get value for the key.
        # Request:
        #   GET {"key": "some key"}
        # Response:
        #   GET_RESP {"key": "some key", "value": "value for this key"}
        #   GET_RESP {"key": "some key", "value": null} - if record for this key is not found
        if msg.type == 'GET':
            # print('GET', msg['key'])
            key = msg['key']
            value = self._data.get(key)
            resp = Message('GET_RESP', {
                'key': key,
                'value': value
            })
            if value is None:
                hash_value = self.get_hash_value(key)
                get_node = self.get_id_node(hash_value)
                if get_node != self._id:
                    new_msg = Message(
                        'GET', {'key': key, 'id_owner': self._id})
                    ctx.send(new_msg, get_node)
                else:
                    ctx.send_local(resp)

                # if hash_value > int(self._id) and self._next_node != self._id :
                #     print(1, self._id, self._next_node)
                #     new_msg = Message('GET_N', {'key' : key, 'id_owner': self._id})
                #     ctx.send(new_msg, self._next_node)
                # elif hash_value <= int(self._id) and self._prev_node != self._id:
                #     print(2, self._id, self._prev_node)
                #     new_msg = Message('GET_P', {'key' : key, 'id_owner': self._id})
                #     ctx.send(new_msg, self._prev_node)
                # else:
                #     print('GET', 1)
                #     ctx.send_local(resp)
            else:
                ctx.send_local(resp)

        # Store (key, value) record.
        # Request:
        #   PUT {"key": "some key", "value: "some value"}
        # Response:
        #   PUT_RESP {"key": "some key", "value: "some value"}
        elif msg.type == 'PUT':
            # print('PUT', msg['key'])
            key = msg['key']
            value = msg['value']
            resp = Message('PUT_RESP', {
                'key': key,
                'value': value
            })

            hash_value = self.get_hash_value(key)
            put_node = self.get_id_node(hash_value)
            if put_node != self._id:
                new_msg = Message(
                    'PUT', {'key': key, 'value': value, 'id_owner': self._id})
                ctx.send(new_msg, put_node)
            else:
                self._data[key] = value
                ctx.send_local(resp)

            # if hash_value > int(self._id):
            #     # print(11, self._id, self._next_node)
            #     new_msg = Message('PUT', {'key': key, 'value': value, 'id_owner' : self._id})
            #     ctx.send(new_msg, self._next_node)
            #     # print(99)
            # else:
            #     # print(12)
            #     # print('PUT', self._id, key, value)
            #     self._data[key] = value
            #     ctx.send_local(resp)

        # Delete value for the key.
        # Request:
        #   DELETE {"key": "some key"}
        # Response:
        #   DELETE_RESP {"key": "some key", "value": "some value"}
        elif msg.type == 'DELETE':
            # print('DELETE', msg['key'])
            key = msg['key']

            if key not in self._data.keys():
                hash_value = self.get_hash_value(key)
                put_node = self.get_id_node(hash_value)
                if put_node != self._id:
                    new_msg = Message(
                        'DELETE', {'key': key, 'id_owner': self._id})
                    ctx.send(new_msg, put_node)
                else:
                    value = self._data.pop(key, None)
                    resp = Message('DELETE_RESP', {'key': key, 'value': value})
                    ctx.send_local(resp)

                # if hash_value > int(self._id) and self._id != self._next_node:
                #     new_msg = Message('DELETE_NEXT', {'key': key, 'id_owner' : self._id})
                #     ctx.send(new_msg, self._next_node)
                # elif hash_value <= int(self._id) and self._id != self._prev_node:
                #     new_msg = Message('DELETE_PREV', {'key': key, 'id_owner' : self._id})
                #     ctx.send(new_msg, self._prev_node)
                # else:
                #     value = self._data.pop(key, None)
                #     resp = Message('DELETE_RESP', {
                #             'key': key,
                #             'value': value
                #     })
                #     ctx.send_local(resp)
            else:
                value = self._data.pop(key, None)
                resp = Message('DELETE_RESP', {
                    'key': key,
                    'value': value
                })
                ctx.send_local(resp)

        # Notification that a new node is added to the system.
        # Request:
        #   NODE_ADDED {"id": "node id"}
        # Response:
        #   N/A
        elif msg.type == 'NODE_ADDED':
            # print('NODE_ADDED', msg['id'], self._nodes)
            self._nodes.add(msg['id'])

            timer_name = 'stabilize_add:'
            timer_name += self._id
            ctx.set_timer(timer_name, 3)

            if self._id != msg['id']:
                nodes = {}
                for key, value in list(self._data.items()):
                    node_id = self.get_id_node(self.get_hash_value(key))
                    if node_id != self._id:
                        self._data.pop(key)
                        if value != None:
                            nodes.setdefault(node_id, []).append(
                                {'key': key, 'value': value})
                for node in nodes.keys():
                    new_msg = Message('PUT_STAB', {'data': nodes[node]})
                    ctx.send(new_msg, node)
            # else:
            #     new_msg = Message('STAB', {'id': self._id})
            #     ctx.send(new_msg, self._next_node)
            #     ctx.send(new_msg, self._prev_node)
        # Notification that a node is removed from the system.
        # Request:
        #   NODE_REMOVED {"id": "node id"}
        # Response:
        #   N/A
        elif msg.type == 'NODE_REMOVED':
            # print('NODE_REMOVED', msg['id'])
            self._nodes.remove(msg['id'])

            # timer_name = 'stabilize_remove:'
            # timer_name += self._id
            # ctx.set_timer(timer_name, 3)

            nodes = {}
            for key, value in list(self._data.items()):
                node_id = self.get_id_node(self.get_hash_value(key))
                if node_id != self._id:
                    self._data.pop(key)
                    if value != None:
                        nodes.setdefault(node_id, []).append({'key': key, 'value': value})
            for node in nodes.keys():
                new_msg = Message('PUT_STAB', {'data': nodes[node]})
                ctx.send(new_msg, node)

        # Get number of records stored on the node.
        # Request:
        #   COUNT_RECORDS {}
        # Response:
        #   COUNT_RECORDS_RESP {"count": 100}
        elif msg.type == 'COUNT_RECORDS':
            resp = Message('COUNT_RECORDS_RESP', {
                'count': len(self._data)
            })
            ctx.send_local(resp)

        # Get keys of records stored on the node.
        # Request:
        #   DUMP_KEYS {}
        # Response:
        #   DUMP_KEYS_RESP {"keys": ["key1", "key2", ...]}
        elif msg.type == 'DUMP_KEYS':
            resp = Message('DUMP_KEYS_RESP', {
                'keys': list(self._data.keys())
            })
            ctx.send_local(resp)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # Implement node-to-node communication using any message types
        # print(self._id)
        if msg.type == "DELETE":
            value = self._data.pop(msg['key'], None)
            resp = Message('DELETE_RESP', {
                'key': msg['key'],
                'value': value
            })
            ctx.send(resp, msg['id_owner'])
        elif msg.type == "GET":
            key = msg['key']
            value = self._data.get(key)

            resp = Message('GET_RESP', {
                'key': key,
                'value': value
            })
            ctx.send(resp, msg['id_owner'])
        elif msg.type == 'GET_RESP':
            ctx.send_local(msg)
        elif msg.type == 'PUT_RESP':
            ctx.send_local(msg)
        elif msg.type == 'DELETE_RESP':
            ctx.send_local(msg)
        elif msg.type == 'PUT':
            self._data[msg['key']] = msg['value']
            resp = Message('PUT_RESP', {
                'key': msg['key'],
                'value': msg['value']
            })
            ctx.send(resp, msg['id_owner'])
        elif msg.type == 'PUT_STAB':
            for el in msg['data']:
                self._data[el['key']] = el['value']
        # elif msg.type == 'STAB':
        #     if msg['id'] not in self._nodes:
        #         self._nodes.add(msg['id'])
        #     nodes = {}
        #     for key, value in list(self._data.items()):
        #         node_id = self.get_id_node(self.get_hash_value(key))
        #         if node_id != self._id:
        #             self._data.pop(key)
        #             nodes.setdefault(node_id, []).append(
        #                 {'key': key, 'value': value})
        #     for node in nodes.keys():
        #         new_msg = Message('PUT_STAB', {'data': nodes[node]})
        #         ctx.send(new_msg, node)

    def on_timer(self, timer_name: str, ctx: Context):
        nodes = {}
        for key, value in list(self._data.items()):
            node_id = self.get_id_node(self.get_hash_value(key))
            if node_id != self._id:
                self._data.pop(key)
                nodes.setdefault(node_id, []).append(
                    {'key': key, 'value': value})
        for node in nodes.keys():
            new_msg = Message('PUT_STAB', {'data': nodes[node]})
            ctx.send(new_msg, node)
