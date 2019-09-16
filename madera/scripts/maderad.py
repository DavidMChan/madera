
import logging
import os
import platform
import subprocess
import sys
import tempfile
import time

import jinja2
import click


@click.command()
@click.option('--port', default='9091', help='The port to launch the madera kafka broker on')
@click.option('--zookeeper-port', default='2181', help='The port to launch the kafka zookeeper instance on')
@click.option('--consumers-only')
@click.option('--data-dir', default=None, help='The root data directory')
def main(**kwargs):

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
    kafka_bin_path = os.path.join(os.path.dirname(__file__), '..', 'kafka', 'kafka_2.12-2.3.0','bin')
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

        # Launch the zookeeper instance
        logging.info('Launching zookeeper instance on port %s', port)
        cmd = 'exec '
        cmd += str(os.path.join(kafka_bin_path, 'zookeeper-server-start.sh'))
        cmd += ' {}'.format(zookeeper_temp.name)
        cmd += ' > {} 2>&1'.format(os.path.join(data_dir, 'zookeeper.log'))
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        zookeeper_processes.append(process)

        # Wait for zookeeper to start up
        time.sleep(5)

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

        logging.info('Launching kafka instance on port %s', port)
        cmd = 'exec '
        cmd += str(os.path.join(kafka_bin_path, 'kafka-server-start.sh'))
        cmd += ' {}'.format(kafka_temp.name)
        cmd += ' > {} 2>&1'.format(os.path.join(data_dir, 'kafka.log'))
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, preexec_fn=os.setsid)
        kafka_processes.append(process)

        print('Finished launch! Running on port: {}'.format(port))

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
            kafka_temp.close()
        except NameError:
            pass

        for p in kafka_processes:
            p.kill()
        print('Finished cleaning up Kafka.')
        print('Waiting one moment before cleaning up zookeeper...')
        time.sleep(5)
        for p in zookeeper_processes:
            p.kill()
        print('Finished cleaning up zookeeper.')



if __name__ == '__main__':
    main()