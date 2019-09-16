
import logging
import os
import platform
import subprocess
import sys
import tempfile
import time
import random
import string
import multiprocessing
import traceback

import jinja2
import click

from madera.consumer.topic_watcher import launch


class Process(multiprocessing.Process):
    def __init__(self, *args, **kwargs):
        multiprocessing.Process.__init__(self, *args, **kwargs)
        self._pconn, self._cconn = multiprocessing.Pipe()
        self._exception = None

    def run(self):
        try:
            multiprocessing.Process.run(self)
            self._cconn.send(None)
        except Exception as e:  # pylint: disable=broad-except
            tb = traceback.format_exc()
            self._cconn.send((RuntimeError(str(e)), tb))

    @property
    def exception(self):
        if self._pconn.poll():
            self._exception = self._pconn.recv()
        return self._exception


@click.command()
@click.option('--port', default='9091', help='The port to launch the madera kafka broker on')
@click.option('--zookeeper-port', default='2181', help='The port to launch the kafka zookeeper instance on')
@click.option('--consumers-only')
@click.option('--data-dir', default=None, help='The root data directory')
def main(**kwargs):  # pylint: disable=too-many-locals,too-many-statements,too-many-branches

    logging.basicConfig(level=logging.DEBUG)

    # Check server compatibility
    if not platform.java_ver:
        logging.error('Unable to launch kafka brokers: Please install a version of Java > 8.0')
        sys.exit(1)

    data_dir = kwargs['data_dir'] if kwargs['data_dir'] is not None else os.getcwd()

    # Create a variable for tracking running subprocesses
    kafka_processes = []
    zookeeper_processes = []

    # Launch the zookeeper broker on the correct port
    # ./zookeeper-server-start.sh ../config/zookeeper.properties
    kafka_bin_path = os.path.join(os.path.dirname(__file__), '..', 'kafka', 'kafka_2.12-2.3.0', 'bin')
    local_bin_path = os.path.dirname(__file__)

    logging.info('Configuring zookeeper')
    with open(os.path.join(local_bin_path, 'templates', 'zookeeper.jinja'), 'r') as jf:
        zookeeper_template = jinja2.Template(jf.read())

    try:
        zookeeper_temp = tempfile.NamedTemporaryFile('w+')

        # Validate the zookeeper config options
        try:
            port = int(kwargs['zookeeper_port'])
        except ValueError:
            logging.error('Unable to launch zookeeper: Invalid port %s', kwargs['zookeeper_port'])
            sys.exit(1)

        # Write the zookeeper properties configuration file
        zookeeper_temp.write(zookeeper_template.render(client_port=kwargs['zookeeper_port']))
        zookeeper_temp.flush()
        zookeeper_temp.file.close()

        # Launch the zookeeper instance
        logging.info('Launching zookeeper instance on port %s', port)
        cmd = 'exec '
        cmd += str(os.path.join(kafka_bin_path, 'zookeeper-server-start.sh'))
        cmd += ' {}'.format(zookeeper_temp.name)
        cmd += ' > {} 2>&1'.format(os.path.join(data_dir, 'zookeeper.log'))
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        zookeeper_processes.append(process)

        # Wait for zookeeper to start up
        print('Waiting for zookeeper to start', end='')
        sys.stdout.flush()
        for _ in range(10):
            print('.', end='')
            sys.stdout.flush()
            time.sleep(1)
        print()

        # Write the Kafka JAAS
        logging.info('Creating Kafka JAAS file')
        api_key = ''.join(random.choice(string.ascii_lowercase) for _ in range(16))
        with open(os.path.join(local_bin_path, 'templates', 'kafka_server_jaas.jinja'), 'r') as jf:
            jaas_template = jinja2.Template(jf.read())
        jaas_temp = tempfile.NamedTemporaryFile('w+')
        jaas_temp.write(jaas_template.render(user_password=api_key))
        jaas_temp.flush()
        jaas_temp.file.close()

        # Write the server-start-sh
        logging.info('Creating Kafka server start script')
        with open(os.path.join(local_bin_path, 'templates', 'kafka-server-start.sh.jinja'), 'r') as jf:
            kss_template = jinja2.Template(jf.read())
        kss_temp = tempfile.NamedTemporaryFile('w+')
        kss_temp.write(kss_template.render(kafka_jaas=jaas_temp.name, kafka_bindir=kafka_bin_path))
        kss_temp.flush()
        kss_temp.file.close()

        # Launch the Kafka broker
        # Write the kafka server properties
        logging.info('Configuring Kafka')
        with open(os.path.join(local_bin_path, 'templates', 'server.jinja'), 'r') as jf:
            kafka_template = jinja2.Template(jf.read())

        kafka_temp = tempfile.NamedTemporaryFile('w+')

        # Validate the zookeeper config options
        try:
            port = int(kwargs['port'])
        except ValueError:
            logging.error('Unable to launch Kafka broker: Invalid port %s', kwargs['port'])
            sys.exit(1)

        # Write the zookeeper properties configuration file
        kafka_temp.write(kafka_template.render(zookeeper_port=kwargs['zookeeper_port'], kafka_port=kwargs['port']))
        kafka_temp.flush()
        kafka_temp.file.close()

        logging.info('Launching kafka instance on port %s', port)
        cmd = 'chmod +x {} && exec '.format(kss_temp.name)
        cmd += kss_temp.name
        cmd += ' {}'.format(kafka_temp.name)
        cmd += ' > {} 2>&1'.format(os.path.join(data_dir, 'kafka.log'))
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, preexec_fn=os.setsid)  # pylint:disable=subprocess-popen-preexec-fn
        kafka_processes.append(process)

        # Launch the madera topic watcher
        logging.info('Launching madera announce consumer')
        madera_announce = Process(target=launch, args=(api_key, port, data_dir,))
        madera_announce.start()

        print('Finished launch! Running on port: {}'.format(port))
        print('API Key: {}'.format(api_key))

        # Wait for the announce thread to join
        madera_announce.join()
        if madera_announce.exception:
            _, tb = madera_announce.exception
            print(tb)
            sys.exit(1)

        # Wait for and clean up returned processse
        for process in kafka_processes + zookeeper_processes:
            process.wait()
            if process.returncode != 0:
                raise subprocess.CalledProcessError(returncode=process.returncode,
                                                    cmd=cmd)
    finally:
        print('\nCleaning up Kafka servers...')
        # Cleanup the temp files
        try:
            zookeeper_temp.close()
        except NameError:
            pass
        try:
            jaas_temp.close()
        except NameError:
            pass
        try:
            kss_temp.close()
        except NameError:
            pass
        try:
            kafka_temp.close()
        except NameError:
            pass

        # Kill the Kafka processes cleanly
        for p in kafka_processes:
            p.kill()
        print('Finished cleaning up Kafka.')

        # Kill the zookeeper processes cleanly
        print('Waiting one moment before cleaning up zookeeper...')
        time.sleep(5)
        for p in zookeeper_processes:
            p.kill()
        print('Finished cleaning up zookeeper.')


if __name__ == '__main__':
    main()
