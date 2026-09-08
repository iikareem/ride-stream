import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { Kafka, logLevel, Producer, Consumer } from 'kafkajs';
import { kafkaConfig } from './kafka.config';

export type CreateConsumerOptions = {
  /**
   * When true, only committed transactional records are visible.
   * Maps to KafkaJS `readUncommitted: false`.
   */
  readCommitted?: boolean;
};

@Injectable()
export class KafkaService implements OnModuleDestroy {
  private readonly kafka = new Kafka({
    clientId: kafkaConfig.clientId,
    brokers: [...kafkaConfig.brokers],
    logLevel: logLevel.ERROR,
  });

  private producer: Producer | null = null;
  private transactionalProducers = new Map<string, Producer>();

  /** Shared non-transactional idempotent producer (GPS simulator, etc.). */
  async createProducer(): Promise<Producer> {
    if (!this.producer) {
      this.producer = this.kafka.producer({
        allowAutoTopicCreation: false,
        // Broker assigns PID + per-partition sequence numbers; retries won't create dup records
        idempotent: true,
        // Required ≤ 5 when idempotent (KafkaJS enforces this)
        maxInFlightRequests: 5,
      });
      await this.producer.connect();
    }
    return this.producer;
  }

  /**
   * Transactional producer for EOS consume→produce (e.g. ETA).
   * One transactionalId → one live instance (zombie fencing on restart).
   */
  async createTransactionalProducer(transactionalId: string): Promise<Producer> {
    const existing = this.transactionalProducers.get(transactionalId);
    if (existing) {
      return existing;
    }

    const producer = this.kafka.producer({
      allowAutoTopicCreation: false,
      idempotent: true,
      maxInFlightRequests: 1,
      transactionalId,
    });
    await producer.connect();
    this.transactionalProducers.set(transactionalId, producer);
    return producer;
  }

  async createConsumer(
    groupId: string,
    options: CreateConsumerOptions = {},
  ): Promise<Consumer> {
    const readCommitted = options.readCommitted !== false;
    const consumer = this.kafka.consumer({
      groupId,
      sessionTimeout: kafkaConfig.sessionTimeoutMs,
      heartbeatInterval: kafkaConfig.heartbeatIntervalMs,
      rebalanceTimeout: kafkaConfig.rebalanceTimeoutMs,
      maxWaitTimeInMs: kafkaConfig.maxWaitTimeInMs,
      minBytes: kafkaConfig.fetchMinBytes,
      // false → READ_COMMITTED (skip aborted txn records)
      readUncommitted: !readCommitted,
    });
    await consumer.connect();
    return consumer;
  }

  async onModuleDestroy(): Promise<void> {
    if (this.producer) {
      await this.producer.disconnect();
      this.producer = null;
    }
    for (const producer of this.transactionalProducers.values()) {
      await producer.disconnect();
    }
    this.transactionalProducers.clear();
  }
}
