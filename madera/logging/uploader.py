import time
import random
import string
import atexit

import msgpack
from confluent_kafka import Producer

from madera.globals import MType


class Uploader():
    def __init__(self, host, port, experiment, run_id):

        # Store some variables
        self.host = host
        self.port = port
        self.experiment = experiment
        self.run_id = run_id
        self.rank_id = ''.join(
            random.choice(string.ascii_lowercase) for i in range(8))

        # Connect to the Kafka broker
        self.kafka_producer = Producer({
            'bootstrap.servers':
            self.host + ':' + str(self.port),
        })

        # Announce the run
        announcement = {
            'type': MType.ANNOUNCE_CREATE,
            'experiment': self.experiment,
            'run_id': self.run_id,
            'rank_id': self.rank_id,
        }
        self.kafka_producer.produce('announce',
                                    key=str(time.time()),
                                    value=msgpack.packb(announcement))
        self.kafka_producer.flush(30)

        # Register the at_exit death call
        atexit.register(self.cleanup)

    def __call__(self, frame):
        # Publish the frame on the topic
        self.kafka_producer.produce(str(self.run_id),
                                    key=str(time.time()),
                                    value=msgpack.packb(frame))

    def flush(self, timeout=10):
        self.kafka_producer.flush(timeout)

    def cleanup(self, ):
        announcement = {
            'type': MType.ANNOUNCE_DIE,
            'experiment': self.experiment,
            'run_id': self.run_id,
            'rank_id': self.rank_id,
        }
        self.kafka_producer.produce('announce',
                                    key=str(time.time()),
                                    value=msgpack.packb(announcement))
        self.kafka_producer.flush()
