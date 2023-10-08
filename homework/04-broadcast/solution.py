from dslabmp import Context, Message, Process
from typing import List
from typing import Any, Dict, List, Tuple, Union
from collections import defaultdict
import time


class BroadcastProcess(Process):

    def __init__(self, proc_id: str, processes: List[str]):
        self._id = proc_id
        self._processes = processes
        self._delivered = []
        self._pending = defaultdict(list)
        self._locals = []
        self._waiting = defaultdict(list)
        self._count = 0
        self._own_send_index = 0
        self._clock = [0] * len(self._processes)

    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type == 'SEND':
            msg_id = str(self._count)
            self._count += 1
            msg_id += ':'
            msg_id += str(self._id)

            self._clock[int(self._id)] += 1
            timestamp = ''.join(str(elem) for elem in self._clock)

            bcast_msg = Message('BCAST', {
                'text': msg['text'],
                'unique_msg_id': msg_id,
                'main_sender': self._id,
                'timestamp': timestamp
            })

            for proc in self._processes:
                ctx.send(bcast_msg, proc)

        elif msg.type == 'DELIVER':
            bcast_msg = Message('BCAST', {
                'text': msg['text'],
                'unique_msg_id': msg['unique_msg_id'],
                'main_sender': msg['main_sender'],
                'timestamp': msg['timestamp']
            })

            for proc in self._processes:
                ctx.send(bcast_msg, proc)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type == 'BCAST':

            deliver_msg = Message('DELIVER', {
                'text': msg['text'],
                'unique_msg_id': msg['unique_msg_id'],
                'main_sender': msg['main_sender'],
                'timestamp': msg['timestamp']
            })

            self._pending[msg['unique_msg_id']].append(sender)

            if msg['unique_msg_id'] not in self._delivered:
                self._delivered.append(msg['unique_msg_id'])
                if self._id != sender:
                    self.on_local_message(deliver_msg, ctx)

            if msg['unique_msg_id'] not in self._locals:
                if len(self._pending[msg['unique_msg_id']]) >= len(self._processes) // 2 + 1:
                    if msg['main_sender'] == self._id:
                        if int(msg['timestamp'][int(self._id)]) - 1 == self._own_send_index:
                            self._own_send_index = int(
                                msg['timestamp'][int(self._id)])
                            ctx.send_local(deliver_msg)
                            self._locals.append(msg['unique_msg_id'])
                        else:
                            self._waiting[msg['unique_msg_id']] = deliver_msg
                        while True:
                            break_flag = 0
                            buffer = list(self._waiting.keys())
                            for el in buffer:
                                if self._waiting[el]['main_sender'] == self._id:
                                    if int(self._waiting[el]['timestamp'][int(self._id)]) - 1 == self._own_send_index:
                                        self._own_send_index = int(
                                            self._waiting[el]['timestamp'][int(self._id)])
                                        ctx.send_local(self._waiting[el])
                                        self._locals.append(el)
                                        self._waiting.pop(el)
                                        break_flag = 1
                            if break_flag == 0 or len(list(self._waiting.keys())) == 0:
                                break
                    else:
                        if msg['unique_msg_id'] not in self._waiting.keys():
                            cur = ''.join(str(elem) for elem in self._clock)
                            flag = 0
                            for i in range(len(self._processes)):
                                if i != int(sender):
                                    if self._clock[i] < int(msg['timestamp'][i]):
                                        flag = 1
                            if self._clock[int(sender)] == int(msg['timestamp'][int(sender)]) - 1 and flag == 0 and int(msg['timestamp'][int(self._id)]) <= self._own_send_index:
                                ctx.send_local(deliver_msg)
                                self._locals.append(msg['unique_msg_id'])
                                self._clock[int(sender)] += 1
                            else:
                                self._waiting[msg['unique_msg_id']
                                              ] = deliver_msg

                    while True:
                        end_flag = 0
                        buffer = list(self._waiting.keys())
                        for el in buffer:
                            if self._waiting[el]['main_sender'] != self._id:
                                flag = 0
                                for i in range(len(self._processes)):
                                    if i != int(self._waiting[el]['main_sender']):
                                        if self._clock[i] < int(self._waiting[el]['timestamp'][i]):
                                            flag = 1
                                if self._clock[int(self._waiting[el]['main_sender'])] == int(self._waiting[el]['timestamp'][int(self._waiting[el]['main_sender'])]) - 1 and flag == 0 and int(self._waiting[el]['timestamp'][int(self._id)]) <= self._own_send_index:
                                    ctx.send_local(self._waiting[el])
                                    self._locals.append(el)
                                    self._clock[int(
                                        self._waiting[el]['main_sender'])] += 1
                                    self._waiting.pop(el)
                                    end_flag = 1
                        if end_flag == 0 or len(list(self._waiting.keys())) == 0:
                            break
