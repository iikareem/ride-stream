import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { Kafka, logLevel, Producer, Consumer } from 'kafkajs';
import { kafkaConfig } from './kafka.config';

@Injectable()
export class KafkaService implements OnModuleDestroy {
  private readonly kafka = new Kafka({
    clientId: kafkaConfig.clientId,
    brokers: [...kafkaConfig.brokers],
    logLevel: logLevel.ERROR,
  });

  private producer: Producer | null = null;

  async createProducer(): Promise<Producer> {
    if (!this.producer) {
      this.producer = this.kafka.producer({
        allowAutoTopicCreation: false,
      });
      await this.producer.connect();
    }
    return this.producer;
  }

  async createConsumer(groupId: string): Promise<Consumer> {
    const consumer = this.kafka.consumer({
      groupId,
      sessionTimeout: kafkaConfig.sessionTimeoutMs,
      heartbeatInterval: kafkaConfig.heartbeatIntervalMs,
      rebalanceTimeout: kafkaConfig.rebalanceTimeoutMs,
      maxWaitTimeInMs: kafkaConfig.maxWaitTimeInMs,
      minBytes: kafkaConfig.fetchMinBytes,
    });
    await consumer.connect();
    return consumer;
  }

  async onModuleDestroy(): Promise<void> {
    if (this.producer) {
      await this.producer.disconnect();
      this.producer = null;
    }
  }
}
