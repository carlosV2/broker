from setuptools import setup

setup(
    name='broker',
    packages=['broker'],
    description='Broker abstraction to connect to AMQP and MQTT brokers.',
    version='0.1',
    url='https://github.com/carlosV2/broker',
    author='Carlos Ortega',
    author_email='carlosV2.0@gmail.com',
    keywords=['pip','broker','amqp', 'mqtt', 'queue'],
    install_requires=['paho-mqtt>=1.6.1', 'pika>=1.2.0']
)
