# Broker

This is a broker abstraction. It's purpose is to facilitate connections to queues like
RabbitMQ (AMQP) and MQTT.

Use the following to instatiate a broker:
```python
broker = Broker.from_dsn('<protocol>://<user>:<password>@<host>:<port>/<divsion>')
```

Currently it only supports 2 protocols:
- AMQP
- MQTT

The `division` parameter is optional but, if set, it defines the exchange to publish
into when using AMQP or the name of the client when using MQTT.

## API

This project is extremely simple and it might not meet all developing needs. For example,
code-defined AMQP structures are unsupported. This project assumes the structures are
defined on beforehand.

Once the broker is initialised, the following methods can be called:
- `publish(message: broker.Messagee)`: It takes the message (containing the topic and the
  payload) and delivers it to the broker.
- `subscribe(queue: str, callback: Callable[[broker.Message], None])`: It subscribes to the given
  queue/topic. Once a message is received, the callback is called with an instance of it.
- `unsubscribe(queue: str)`: It unsubscribes from the given queue/topic.
- `consume_forever()`: It starts consuming messages from the subscribed queue/topic. Attention: this
  is a blocking call!

## Manual PIP installation
Use the following command:
```
pip install git+https://github.com/carlosV2/broker
```

## Automatic PIP installation through requirements.txt
Use the following entry:
```
git+https://github.com/carlosV2/broker
```
