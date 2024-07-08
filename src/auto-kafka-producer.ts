import { OnModuleInit, OnModuleDestroy, Provider } from '@nestjs/common';
import { EventEmitter2, OnEvent } from '@nestjs/event-emitter';
import { MessageBatcher } from '@raphaabreu/message-batcher';
import { PromiseCollector } from '@raphaabreu/promise-collector';
import { StructuredLogger } from '@raphaabreu/nestjs-opensearch-structured-logger';
import { CompressionTypes, Kafka, Message, Producer } from 'kafkajs';

export type AutoKafkaProducerOptions<TEvent, TValue, TKey> = {
  name?: string;
  topicName: string;
  eventName: string;
  batchSize?: number;
  maxBatchIntervalMs?: number;
  verboseBeginning?: boolean;

  keyExtractor?: (event: TEvent) => TKey;
  valueExtractor?: (event: TEvent) => TValue;
  keySerializer?: (event: TKey) => string;
  valueSerializer?: (event: TValue) => string;

  prepareMessage?: (event: TEvent) => Message;

  sample?: number | ((event: TEvent) => boolean);

  acks?: number;
  timeout?: number;
  compression?: CompressionTypes;
};

export function defaultSerializer(value: any): string {
  if (value === null) {
    return 'null';
  } else if (value === undefined) {
    return 'undefined';
  } else if (typeof value !== 'object') {
    return value.toString();
  } else {
    return JSON.stringify(value);
  }
}

const defaultOptions: Partial<AutoKafkaProducerOptions<any, any, any>> = {
  batchSize: 1000,
  maxBatchIntervalMs: 10000,
  keyExtractor: () => null,
  valueExtractor: (e) => e,
  keySerializer: defaultSerializer,
  valueSerializer: defaultSerializer,
  verboseBeginning: true,

  sample: 1,
  acks: 0,
};

const MAX_VERBOSE_LOG_COUNT = 10;

export class AutoKafkaProducer<TEvent, TValue, TKey> implements OnModuleInit, OnModuleDestroy {
  private readonly logger: StructuredLogger;
  private readonly batcher: MessageBatcher<TEvent>;
  private readonly promiseCollector = new PromiseCollector();
  private readonly options: AutoKafkaProducerOptions<TEvent, TValue, TKey>;
  private readonly producer: Producer;
  private connectionPromise: Promise<void>;

  private verboseLogCount = 0;

  public static register<TEvent, TValue, TKey>(options: AutoKafkaProducerOptions<TEvent, TValue, TKey>): Provider {
    return {
      provide: AutoKafkaProducer.getServiceName(options.name || options.eventName),
      useFactory: (kafka: Kafka, eventEmitter: EventEmitter2) => new AutoKafkaProducer(kafka, eventEmitter, options),
      inject: [Kafka, EventEmitter2],
    };
  }

  public static getServiceName(name: string): string {
    return `${AutoKafkaProducer.name}:${name}`;
  }

  constructor(kafka: Kafka, eventEmitter: EventEmitter2, options: AutoKafkaProducerOptions<TEvent, TValue, TKey>) {
    this.options = { ...defaultOptions, ...options };

    const name = this.options.name || this.options.eventName;

    if (this.options.eventName === undefined) {
      throw new Error(`eventName is required in ${name}`);
    }
    if (this.options.topicName === undefined) {
      throw new Error(`topicName is required in ${name}`);
    }

    this.logger = new StructuredLogger(AutoKafkaProducer.getServiceName(name));

    this.batcher = new MessageBatcher(
      this.options.batchSize,
      this.promiseCollector.wrap((b) => this.publishBatch(b)),
    );

    eventEmitter.on(this.options.eventName, (e) => this.onEvent(e));

    this.producer = kafka.producer();
  }

  onEvent(event: TEvent) {
    if (typeof this.options.sample === 'number') {
      // If sample is a number and it's greater than the random number, then we don't want to publish this event
      if (Math.random() > this.options.sample) {
        return;
      }
    } else if (!this.options.sample(event)) {
      // If sample is a function and it returns false, then we don't want to publish this event
      return;
    }

    this.batcher.add(event);
  }

  prepareMessages(events: TEvent[]): Message[] {
    if (this.options.prepareMessage) {
      return events.map(this.options.prepareMessage);
    }

    return events.map((event) => {
      const key = this.options.keyExtractor(event);
      const value = this.options.valueExtractor(event);

      return {
        key: key ? this.options.keySerializer(key) : undefined,
        value: this.options.valueSerializer(value),
      };
    });
  }

  async publishBatch(events: TEvent[]) {
    const params = {
      topic: this.options.topicName,
      messages: this.prepareMessages(events),
      acks: this.options.acks,
      timeout: this.options.timeout,
      compression: this.options.compression,
    };

    await this.connectionPromise;

    try {
      await this.producer.send(params);

      const verboseLog = this.verboseLoggingEnabled();

      this.logger
        .createScope({
          messages: !this.options.verboseBeginning
            ? '-'
            : verboseLog
            ? JSON.stringify(params.messages)
            : `messages are only logged for the first ${MAX_VERBOSE_LOG_COUNT} batches`,
        })
        [verboseLog ? 'log' : 'debug'](
          'Published ${messageCount} messages to Kafka topic ${topicName}: ${successCount} succeeded, ${failCount} failed',
          params.messages.length,
          this.options.topicName,
          params.messages.length,
          0,
        );

      this.countVerboseLogging();
    } catch (error) {
      this.logger.error(
        'Failed to publish ${messageCount} messages to Kafka topic ${topicName}: ${errorMessage}',
        error,
        params.messages.length,
        this.options.topicName,
        error.message,
      );
    }
  }

  onModuleInit() {
    this.logger.log(
      'Starting message batcher with batchSize = ${batchSize} and maxBatchIntervalMs = ${maxBatchIntervalMs}ms...',
      this.options.batchSize,
      this.options.maxBatchIntervalMs,
    );

    this.batcher.start(this.options.maxBatchIntervalMs);

    this.connectionPromise = this.producer.connect();
    this.promiseCollector.add(this.connectionPromise);
  }

  async onModuleDestroy() {
    this.logger.log('Stopping message batcher...');

    this.batcher.stop();
    await this.flush();

    await this.producer.disconnect();
  }

  @OnEvent('flush')
  async flush() {
    this.batcher.flush();
    await this.promiseCollector.pending();

    this.logger[this.verboseLoggingEnabled() ? 'log' : 'debug']('Flushed');
    this.countVerboseLogging();
  }

  private verboseLoggingEnabled() {
    return this.options.verboseBeginning && this.verboseLogCount < MAX_VERBOSE_LOG_COUNT;
  }

  private countVerboseLogging() {
    if (this.verboseLoggingEnabled()) {
      this.verboseLogCount++;
      if (this.verboseLogCount === MAX_VERBOSE_LOG_COUNT) {
        this.logger.log('Success messages will be logged as debug from now on');
      }
    }
  }
}
