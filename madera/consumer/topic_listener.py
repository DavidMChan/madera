import logging
import signal
import os

import msgpack

from confluent_kafka import Consumer


class GracefulKiller:
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        logging.warning('Recieved signal %s in %s', signum, frame)
        self.kill_now = True


def launch_topic_listener(exp, api_key, run, port):
    killer = GracefulKiller()

    consumer = Consumer({
        'bootstrap.servers': 'localhost:' + str(port),
        'group.id': 0,
        'auto.offset.reset': 'earliest',
        'sasl.username': 'admin',
        'sasl.password': api_key,
        'security.protocol': 'sasl_plaintext',
        'sasl.mechanism': 'PLAIN',
    })
    consumer.subscribe([run['run_id']])
    with open(os.path.join(run['run_directory'], 'run.log'), 'w+') as run_log:
        while not killer.kill_now:

            msg = consumer.poll(0.1)

            # Validate the message is good
            if msg is None:
                continue
            if msg.error():
                logging.error('Topic Listener Error: %s', msg.error())
                continue

            message = msgpack.loads(msg.value(), encoding='utf-8')
            for m in message:
                run_log.write(str(m) + '\n')
            run_log.flush()
