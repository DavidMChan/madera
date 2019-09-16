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
import time

import msgpack
import tinydb

from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from madera.globals import MType
from madera.util import mkdir_p, sanitize_path
from madera.consumer.topic_listener import launch_topic_listener

_PROC_DB = {}  # type: ignore


def process_message(msg, run_db, data_directory, api_key, port, adm_cli):

    global _PROC_DB  # pylint: disable=global-statement

    # Parse the message
    message = msgpack.loads(msg, encoding='utf-8')

    if 'type' not in message:
        logging.error('Invalid message recieved')
        return

    if message['type'] == MType.ANNOUNCE_CREATE.value:
        # Register the experiment and run
        # Create and register the experiment
        if not process_create_message(message, run_db, data_directory, api_key, port):
            return
    elif message['type'] == MType.ANNOUNCE_DIE.value:
        process_disconnect_message(message, run_db, adm_cli)


def process_disconnect_message(message, run_db, adm_cli):

    time.sleep(5)

    # Get the experiment
    experiment_table = run_db.table('experiments')
    experiment_name = sanitize_path(message['experiment'])
    exp = experiment_table.search(tinydb.where('experiment') == experiment_name)
    if exp:
        exp = exp[0]
    else:
        logging.warning(
            'Tried to remove process from an experiment, but encountered an error in which the experiment is not real.')

    try:
        _PROC_DB[exp['experiment']][message['run_id']]['producers'].remove(message['rank_id'])
    except KeyError:
        logging.warning(
            'Tried to remove rank producer from an experiment, but encountered an error in which the producer is not real.'
        )

    if not _PROC_DB[exp['experiment']][message['run_id']]['producers']:
        # Remove the topic for the run
        def remove_runid():
            time.sleep(30)

            atexit.unregister(_PROC_DB[exp['experiment']][message['run_id']]['process'].terminate)
            _PROC_DB[exp['experiment']][message['run_id']]['process'].terminate()

            # Update the run DB
            logging.info('Setting runs to finished')
            run_table = run_db.table('runs')
            run_table.update({'finished': True}, tinydb.where('run_id') == message['run_id'])

            try:
                logging.info('Removing Kafka topic: {}'.format(message['run_id']))
                results = adm_cli.delete_topics([message['run_id']])
                for v in results.values():
                    v.result()
            except KafkaException:
                pass

        multiprocessing.Process(target=remove_runid).start()


def process_create_message(message, run_db, data_directory, api_key, port):
    # Register the experiment and run
    # Create and register the experiment
    if 'experiment' not in message or 'run_id' not in message or 'rank_id' not in message:
        logging.error('Invalid creation announcement recieved')
        return False

    experiment_table = run_db.table('experiments')
    experiment_name = sanitize_path(message['experiment'])
    experiment = experiment_table.search(tinydb.where('experiment') == experiment_name)
    if not experiment:
        logging.info('Creating experiment: %s', experiment_name)
        # Make a directory for the experiment
        data_path = os.path.join(data_directory, experiment_name)
        logging.info('Experiment data located at: %s', data_path)
        mkdir_p(data_path)

        # Create the experiment
        experiment_table.insert({'experiment': experiment_name, 'data_path': data_path})
        exp = experiment_table.search(tinydb.where('experiment') == experiment_name)[0]
    else:
        exp = experiment[0]

    if exp['experiment'] not in _PROC_DB:
        _PROC_DB[exp['experiment']] = {}

    # Create and register the run ID
    run_table = run_db.table('runs')
    runs = run_table.search((tinydb.where('experiment') == experiment_name) &
                            (tinydb.where('run_id') == message['run_id']))
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
            'finished': False
        })
        run = run_table.search((tinydb.where('experiment') == experiment_name) &
                               (tinydb.where('run_id') == message['run_id']))[0]
    else:
        run = runs[0]

    # Start the process
    if message['run_id'] not in _PROC_DB[
            exp['experiment']] or not _PROC_DB[exp['experiment']][message['run_id']].is_alive():
        _PROC_DB[exp['experiment']][message['run_id']] = {
            'process': multiprocessing.Process(target=launch_topic_listener, args=(
                exp,
                api_key,
                run,
                port,
            )),
            'producers': set(),
            'finished': False,
        }
        _PROC_DB[exp['experiment']][message['run_id']]['process'].start()
        atexit.register(_PROC_DB[exp['experiment']][message['run_id']]['process'].terminate)

    _PROC_DB[exp['experiment']][message['run_id']]['producers'].add(message['rank_id'])

    return True


def launch(api_key, port, data_directory=None, topic='announce'):

    logging.basicConfig(level=logging.DEBUG)

    # Initialize the database
    if data_directory is None:
        data_directory = os.getcwd()
    db = tinydb.TinyDB(os.path.join(data_directory, 'run_db.json'))

    logging.info('Constructing local consumer')
    consumer = Consumer({
        'bootstrap.servers': 'localhost:' + str(port),
        'group.id': 0,
        'auto.offset.reset': 'earliest',
        'sasl.username': 'admin',
        'sasl.password': api_key,
        'security.protocol': 'sasl_plaintext',
        'sasl.mechanism': 'PLAIN',
    })

    adm_client = AdminClient({
        'bootstrap.servers': 'localhost:' + str(port),
        'group.id': 0,
        'auto.offset.reset': 'earliest',
        'sasl.username': 'admin',
        'sasl.password': api_key,
        'security.protocol': 'sasl_plaintext',
        'sasl.mechanism': 'PLAIN',
    })

    # Clean up the Kafka board
    try:
        results = adm_client.delete_topics(list(consumer.list_topics().topics.keys()))
        for v in results.values():
            v.result()
    except ValueError:
        pass

    # Create the announce topic
    try:
        logging.info('Setting up announce topic')
        tp_future = adm_client.create_topics([NewTopic('announce', 1, 1)])
        tp_future['announce'].result()  # Wait for the future
        logging.info('Topic created!')
    except KafkaException as ex:
        logging.warning(ex)

    logging.info('Connecting to topic: %s', topic)
    consumer.subscribe([topic])

    # Main consumer loop
    while True:
        msg = consumer.poll(0.1)

        # Validate the message is good
        if msg is None:
            continue
        if msg.error():
            logging.error('Topic Consumer Error: %s', msg.error())
            continue
        logging.info('Processing Message')
        process_message(msg.value(), db, data_directory, api_key, port, adm_client)
