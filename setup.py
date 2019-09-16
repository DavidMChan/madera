# -*- coding: utf-8 -*-
import tarfile
import os

from setuptools import setup, find_packages
from setuptools.command.install import install
from setuptools.command.develop import develop


class PostInstallCommand(install):
    def run(self):
        install.run(self)
        # Unzip the Kafka installation
        tf = tarfile.open(os.path.join(os.path.dirname(__file__), 'madera', 'kafka', 'kafka_2.12-2.3.0.tgz'))
        tf.extractall(os.path.join(os.path.dirname(__file__), 'madera', 'kafka'))


class PostDevelopCommand(develop):
    def run(self):
        develop.run(self)
        # Unzip the Kafka installation
        tf = tarfile.open(os.path.join(os.path.dirname(__file__), 'madera', 'kafka', 'kafka_2.12-2.3.0.tgz'))
        tf.extractall(os.path.join(os.path.dirname(__file__), 'madera', 'kafka'))


with open('README.md', 'r') as rf:
    README = rf.read()

setup(
    name='madera',
    version='1.0',
    description='Distributed logging with Kafka',
    long_description=README,
    long_description_content_type='text/markdown',
    author='David Chan',
    author_email='davidchan@berkeley.edu',
    url='https://github.com/DavidMChan/madera',
    license='GPL-v3',
    install_requires=[
        'confluent-kafka',
        'msgpack',
        'requests',
        'click',
        'jinja2',
        'tinydb',
    ],
    entry_points={
        'console_scripts': [
            'maderad = madera.scripts.maderad:main',
        ]
    },
    cmdclass={'install': PostInstallCommand, 'develop': PostDevelopCommand},
    packages=find_packages(exclude='example')  # exclude=('tests', 'docs')
)
