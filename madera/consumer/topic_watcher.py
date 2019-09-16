"""
This utility watches the "announce" topic on the Kafka broker and launches logging
consumers when a new announcement is made. This utility also is responsible for
maintaining the metadata database, which keeps track of the different runs.

Additionally, this tool reaps dead experiments - and marks them as complete in the
main tab.
"""

import atexit
import datetime
import logging
import multiprocessing
import os

import msgpack
import tinydb

from confluent_kafka import Consumer

from madera.globals import MType
from madera.util import mkdir_p, sanitize_path
from madera.consumer.topic_listener import launch_topic_listener

_PROC_DB = {}

def process_message(msg, run_db, data_directory):

    global _PROC_DB

    # Parse the message
    message = msgpack.loads(msg)
    if 'type' not in message:
        logging.error('Invalid message recieved')
        return

    if message['type'] == MType.ANNOUNCE_CREATE:
        # Register the experiment and run


        # Create and register the experiment
        if 'experiment' not in message or 'run_id' not in message or 'rank_id' not in message:
            logging.error('Invalid creation announcement recieved')
            return

        experiment_table = run_db.table('experiments')
        experiment_name = sanitize_path(message['experiment'])
        experiment = experiment_table.search(tinydb.where('experiment') == experiment_name)
        if not experiment:
            # Make a directory for the experiment
            data_path = os.path.join(data_directory)
            mkdir_p(data_path)

            # Create the experiment
            experiment_table.insert({
                'experiment': experiment_name,
                'data_path': data_path})
            exp = experiment_table.search(tinydb.where('experiment') == experiment_name)
        else:
            exp = experiment[0]

        if exp['experiment'] not in _PROC_DB:
            _PROC_DB[exp['experiment']] = {}

        # Create and register the run ID
        run_table = run_db.table('runs')
        runs = run_table.search(tinydb.where('experiment') == experiment_name & tinydb.where('run_id') == message['run_id'])
        if not runs:
            # Create the run
            run_path = datetime.datetime.now().strftime("%Y-%m-%d--%H-%M-%S")
            run_directory = os.path.join(data_directory, experiment_name, run_path)
            mkdir_p(run_directory)

            # Update the database
            run_table.insert({
                'experiment': experiment_name,
                'run_id': message['run_id'],
                'run_directory': run_directory,
                'finished': False})
            run = run_table.search(tinydb.where('experiment') == experiment_name & tinydb.where('run_id') == message['run_id'])[0]
        else:
            run = runs[0]

        # Start the process
        if message['run_id'] not in _PROC_DB[exp['experiment']] or not _PROC_DB[exp['experiment']][message['run_id']].is_alive():
            _PROC_DB[exp['experiment']][message['run_id']] = {
                'process': multiprocessing.Process(target=launch_topic_listener, args=(exp, run,)),
                'producers': set(),
                'finished': False,
            }
            _PROC_DB[exp['experiment']][message['run_id']]['process'].start()
            atexit.register(_PROC_DB[exp['experiment']][message['run_id']]['process'].terminate)
        else:
            _PROC_DB[exp['experiment']][message['run_id']]['producers'].add(message['rank_id'])

    elif message['type'] == MType.ANNOUNCE_DIE:
        try:
            _PROC_DB[exp['experiment']][message['run_id']]['producers'].remove(message['rank_id'])
        except KeyError:
            logging.warning('Tried to remove process from an experiment, but encountered an error in which the process is not real.')
        if not _PROC_DB[exp['experiment']][message['run_id']]['producers']:
            atexit.unregister(_PROC_DB[exp['experiment']][message['run_id']]['process'].terminate)
            _PROC_DB[exp['experiment']][message['run_id']]['process'].terminate()

            # Update the run DB
            run_table = run_db.table('runs')
            run_table.update({'finished': True}, tinydb.where('run_id') == message['run_id'])



def launch(port, data_directory=None, topic='announce'):

    # Initialize the database
    if data_directory is None:
        data_directory = os.getcwd()
    db = tinydb.TinyDB(os.path.join(data_directory, 'run_db.json'))

    consumer = Consumer({
        'bootstrap.servers': 'localhost' + str(port),
        'auto.offset.reset': 'earliest',
    })

    c.subscribe(topic)

    # Main consumer loop
    while True:
        msg = consumer.poll(1.0)

        # Validate the message is good
        if msg is None:
            continue
        if msg.error():
            logging.error('Topic Consumer Error: %s', msg.error())
            continue

        try:
            process_message(msg.value(), db, data_directory)
        except Exception as ex:
            logging.error('General error processing messages: %s', str(ex))


