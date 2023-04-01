import { Test, TestingModule } from '@nestjs/testing';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { CompressionTypes, Kafka, Producer } from 'kafkajs';
import { AutoKafkaProducer, AutoKafkaProducerOptions } from './auto-kafka-producer';

describe('AutoKafkaProducer', () => {
  let sut: AutoKafkaProducer<any, any, any>;
  let kafka: jest.Mocked<Kafka>;
  let producer: jest.Mocked<Producer>;
  let eventEmitter: EventEmitter2;

  let options: AutoKafkaProducerOptions<any, any, any>;

  beforeEach(async () => {
    producer = {
      connect: jest.fn(),
      send: jest.fn(),
    } as unknown as jest.Mocked<Producer>;

    await makeSut({
      topicName: 'test-topic',
      eventName: 'test-event',

      acks: 123,
      timeout: 456,
      compression: CompressionTypes.LZ4,
    });
  });

  async function makeSut(opts?: AutoKafkaProducerOptions<any, any, any>) {
    options = opts;

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        {
          provide: Kafka,
          useValue: { producer: jest.fn().mockReturnValue(producer) },
        },
        EventEmitter2,
        {
          provide: AutoKafkaProducer,
          useFactory: (k: Kafka, e: EventEmitter2) => new AutoKafkaProducer(k, e, options),
          inject: [Kafka, EventEmitter2],
        },
      ],
    }).compile();

    sut = module.get<AutoKafkaProducer<any, any, any>>(AutoKafkaProducer);
    kafka = module.get<jest.Mocked<Kafka>>(Kafka);
    eventEmitter = module.get<EventEmitter2>(EventEmitter2);
  }

  afterEach(() => {
    jest.resetAllMocks();
  });

  it('should be defined', () => {
    expect(sut).toBeDefined();
  });

  describe('onModuleInit', () => {
    it('should start message batcher and connect producer', () => {
      // Arrange
      const batcherStartSpy = jest.spyOn(sut['batcher'], 'start').mockImplementation(() => {
        // do nothing
      });

      // Act
      sut.onModuleInit();

      // Assert
      expect(batcherStartSpy).toHaveBeenCalledTimes(1);
      expect(batcherStartSpy).toHaveBeenCalledWith(10000);
      expect(producer.connect).toHaveBeenCalledTimes(1);
    });
  });

  describe('onModuleDestroy', () => {
    it('should stop message batcher and flush pending promises', async () => {
      // Arrange
      const batcherStopSpy = jest.spyOn(sut['batcher'], 'stop');
      const flushSpy = jest.spyOn(sut, 'flush');

      // Act
      await sut.onModuleDestroy();

      // Assert
      expect(batcherStopSpy).toHaveBeenCalledTimes(1);
      expect(flushSpy).toHaveBeenCalledTimes(1);
    });
  });

  describe('flush', () => {
    it('should flush the batcher and wait for pending promises', async () => {
      // Arrange
      const batcherFlushSpy = jest.spyOn(sut['batcher'], 'flush');
      const promiseCollectorPendingSpy = jest.spyOn(sut['promiseCollector'], 'pending');

      // Act
      await sut.flush();

      // Assert
      expect(batcherFlushSpy).toHaveBeenCalledTimes(1);
      expect(promiseCollectorPendingSpy).toHaveBeenCalledTimes(1);
    });
  });

  describe('onEvent', () => {
    it('should add the event to the batcher if sampling conditions are met', () => {
      // Arrange
      const testEvent = { data: 'test' };
      const batcherAddSpy = jest.spyOn(sut['batcher'], 'add');

      // Act
      sut.onEvent(testEvent);

      // Assert
      expect(batcherAddSpy).toHaveBeenCalledTimes(1);
      expect(batcherAddSpy).toHaveBeenCalledWith(testEvent);
    });

    it('should not add the event to the batcher if sampling conditions are not met', async () => {
      // Arrange
      const testEvent = { data: 'test' };
      const batcherAddSpy = jest.spyOn(sut['batcher'], 'add');
      await makeSut({ ...options, sample: () => false });

      // Act
      sut.onEvent(testEvent);

      // Assert
      expect(batcherAddSpy).toHaveBeenCalledTimes(0);
    });
  });

  describe('publishBatch', () => {
    it('should publish messages to Kafka and log success', async () => {
      // Arrange
      const testEvents = [{ data: 'test1' }, { data: 'test2' }];
      producer.send.mockResolvedValue(undefined);

      // Act
      await sut['publishBatch'](testEvents);

      // Assert
      expect(producer.send).toHaveBeenCalledTimes(1);
      expect(producer.send).toHaveBeenCalledWith({
        topic: options.topicName,
        messages: [
          {
            key: undefined,
            value: '{"data":"test1"}',
          },
          {
            key: undefined,
            value: '{"data":"test2"}',
          },
        ],
        acks: 123,
        timeout: 456,
        compression: CompressionTypes.LZ4,
      });
    });

    it('should log an error if publishing to Kafka fails', async () => {
      // Arrange
      const testEvents = [{ data: 'test1' }, { data: 'test2' }];
      const error = new Error('Kafka error');
      producer.send.mockRejectedValue(error);
      const logErrorSpy = jest.spyOn(sut['logger'], 'error');

      // Act
      await sut['publishBatch'](testEvents);

      // Assert
      expect(producer.send).toHaveBeenCalledTimes(1);
      expect(logErrorSpy).toHaveBeenCalledWith(
        'Failed to publish messages to Kafka topic ${topicName}',
        error,
        options.topicName,
      );
    });
  });
});
