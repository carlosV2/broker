# Broker

This is a broker abstraction. It's purpose is to facilitate connections to queues like
RabbitMQ (AMQP) and MQTT.

Use the following to instatiate a broker:
```python
broker = Broker.from_dsn('<scheme>://<username>:<password>@<host>:<port>/<path>')
```

Currently it only supports 2 schemes:
- amqp
- mqtt

The `path` parameter is optional but, if set, it defines the exchange to publish
into when using AMQP or the name of the client when using MQTT.

## API

This project is extremely simple and it might not meet all developing needs. For example,
code-defined AMQP structures are unsupported. This project assumes the structures are
defined on beforehand.

Once the broker is initialised, the following methods can be called:
- `publish(message: broker.Messagee)`: It takes the message (containing the topic and the
  payload) and delivers it to the broker.
- `subscribe(topic: str, callback: Callable[[broker.Message], None])`: It subscribes to the given
  topic/queue. Once a message is received, the callback is called with an instance of it.
- `unsubscribe(topic: str)`: It unsubscribes from the given topic/queue.
- `consume_forever()`: It starts consuming messages from the subscribed queue/topic. Attention: this
  is a blocking call!

The `broker.Message` object can be created with any of the following methods:
- `Message.from_raw_payload(topic: str, payload: bytes)`
- `Message.from_decoded_payload(topic: str, payload: str)`
- `Message.from_dehydrated_json_payload(topic: str, payload: Any)`

It also supports the following methods to access it's data:
- `get_topic()`: It returns the topic/routing key it was sent to.
- `get_raw_payload()`: It returns the payload as `bytes`.
- `get_encoded_payload()`: It returns the payload as `str`.
- `get_hydrated_json_payload()`: It returns the payload by parsing the json data.

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
