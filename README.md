# AutoKafkaProducer

AutoKafkaProducer is a NestJS utility that automatically batches and publishes messages to a Kafka topic. It is designed to be flexible and easy to use, with options for custom serialization and message preparation.

The class uses a batching mechanism to improve performance and reduce the number of network calls to Kafka. It also provides options for message serialization, compression, and logging.

## Installation

First, install the package using your package manager:

```bash
npm i @raphaabreu/nestjs-auto-kafka-producer
```

## Usage

To use, import the `AutoKafkaProducer` it into your NestJS module and provide the necessary options.
You can register it as many times as you have Kafka topics to publish to.

```typescript
import { Module } from '@nestjs/common';
import { AutoKafkaProducer } from '@raphaabreu/nestjs-auto-kafka-producer';

@Module({
  providers: [
    AutoKafkaProducer.register({
      eventName: 'event-1',
      topicName: 'topic-event-1',
    }),
    AutoKafkaProducer.register({
      eventName: 'event-2',
      topicName: 'topic-event-2',
    }),
  ],
})
export class AppModule {}
```

Now, you can emit events with the specified eventName using the `EventEmitter2` instance:

```typescript
eventEmitter.emit('event-1', { foo: 'bar' });
```

The `AutoKafkaProducer` will automatically batch and publish messages to the specified Kafka topic.

### Options

The `AutoKafkaProducerOptions` type is used to configure the producer's behavior. The available options are:

- `topicName`: The name of the Kafka topic to publish messages to. Required.
- `eventName`: The name of the event to listen for. Required.
- `batchSize`: The maximum number of messages to include in a batch. Defaults to 1000.
- `maxBatchIntervalMs`: The maximum amount of time to wait before publishing a batch, in milliseconds. Defaults to 10000.
- `verboseBeginning`: Whether to log the first 10 messages published to Kafka at the "log" level instead of "debug". Defaults to true.
- `keyExtractor`: A function that takes an event object and returns the key to use for the message. Defaults to a function that returns null.
- `valueExtractor`: A function that takes an event object and returns the value to use for the message. Defaults to a function that returns the event object itself.
- `keySerializer`: A function that takes a key object and returns a string representation of it. Defaults to JSON.stringify.
- `valueSerializer`: A function that takes a value object and returns a string representation of it. Defaults to JSON.stringify.
- `prepareMessage`: A function that takes an event object and returns a Kafka message object. This option overrides `keyExtractor`, `valueExtractor`, `keySerializer`, and `valueSerializer`.
- `sample`: A number between 0 and 1 that determines the probability of publishing a given event. Defaults to 1, meaning all events will be published. Can also be a function that takes an event object and returns a boolean indicating whether to publish the event.
- `acks`: The number of acknowledgements the producer requires the broker to receive before considering a message as sent. Defaults to 0.
- `timeout`: The maximum amount of time the producer will wait for an acknowledgement from the broker, in milliseconds.
- `compression`: The type of compression to use for the message.

## Tests

To run the provided unit tests just execute `npm run tests`.

## License

MIT License

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## Support

If you have any issues or questions, please open an issue on the project repository.
