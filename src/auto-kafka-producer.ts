import { OnModuleInit, OnModuleDestroy, Provider } from '@nestjs/common';
import { EventEmitter2, OnEvent } from '@nestjs/event-emitter';
import { MessageBatcher } from '@raphaabreu/message-batcher';
import { PromiseCollector } from '@raphaabreu/promise-collector';
import { StructuredLogger } from '@raphaabreu/nestjs-opensearch-structured-logger';
import { CompressionTypes, Kafka, Message, Producer } from 'kafkajs';

export type AutoKafkaProducerOptions<TEvent, TValue, TKey> = {
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

const defaultOptions: Partial<AutoKafkaProducerOptions<any, any, any>> = {
  batchSize: 1000,
  maxBatchIntervalMs: 10000,
  keyExtractor: () => null,
  valueExtractor: (e) => e,
  keySerializer: JSON.stringify,
  valueSerializer: JSON.stringify,
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

  private verboseLogCount = 0;

  public static register<TEvent, TValue, TKey>(options: AutoKafkaProducerOptions<TEvent, TValue, TKey>): Provider {
    return {
      provide: AutoKafkaProducer.getServiceName(options.eventName),
      useFactory: (kafka: Kafka, eventEmitter: EventEmitter2) => new AutoKafkaProducer(kafka, eventEmitter, options),
      inject: [Kafka, EventEmitter2],
    };
  }

  public static getServiceName(eventName: string): string {
    return `${AutoKafkaProducer.name}:${eventName}`;
  }

  constructor(
    private readonly kafka: Kafka,
    eventEmitter: EventEmitter2,
    options: AutoKafkaProducerOptions<TEvent, TValue, TKey>,
  ) {
    this.options = { ...defaultOptions, ...options };

    this.logger = new StructuredLogger(AutoKafkaProducer.getServiceName(this.options.eventName));

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

    this.logger.debug(
      'Publishing ${messageCount} messages to Kafka topic ${topicName}...',
      events.length,
      this.options.topicName,
    );

    try {
      await this.producer.send(params);

      const verboseLog = this.verboseLoggingEnabled();

      this.logger[verboseLog ? 'log' : 'debug'](
        'Published ${batchMessages} messages to Kafka topic ${topicName}',
        verboseLog ? JSON.stringify(params.messages) : '{omitted}',
        this.options.topicName,
      );

      this.countVerboseLogging();
    } catch (error) {
      this.logger.error('Failed to publish messages to Kafka topic ${topicName}', error, this.options.topicName);
    }
  }

  onModuleInit() {
    this.logger.log(
      'Starting message batcher with interval ${maxBatchIntervalMs}ms...',
      this.options.maxBatchIntervalMs,
    );

    this.batcher.start(this.options.maxBatchIntervalMs);

    this.producer.connect();
  }

  async onModuleDestroy() {
    this.logger.log('Stopping message batcher...');

    this.batcher.stop();
    await this.flush();
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
