#!/usr/bin/env python

import threading
import time

import redis


source_redis = redis.Redis(password="", host='', port=6379)
destination_redis = redis.Redis(host='127.0.0.1', port=6379)

def transfer_messages(stream_name):
    print(f"Start transfer messages {stream_name} ")
    last_id = '0'

    while True:
        messages = source_redis.xread({stream_name: last_id}, block=0, count=10)

        for message in messages:
            stream, message_data = message
            message_id, fields = message_data[0]

            message_body = {}
            for key, value in fields.items():
                key = key.decode('utf-8')
                message_body[key] = value

            print(f"Received message: {stream_name} {message_body}")

            destination_redis.xadd(stream_name, message_body)
            last_id = message_id

        time.sleep(1)


if __name__ == '__main__':
    streemACC = threading.Thread(target=transfer_messages, args=("ACC",))
    streemTXN = threading.Thread(target=transfer_messages, args=("TXN",))

    streemTXN.start()
    streemACC.start()

    streemTXN.join()
    streemACC.join()
