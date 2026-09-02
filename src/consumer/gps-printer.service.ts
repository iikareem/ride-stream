import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { EachMessagePayload } from 'kafkajs';
import { KafkaService } from '../kafka/kafka.service';
import { kafkaConfig } from '../kafka/kafka.config';

@Injectable()
export class GpsPrinterService implements OnModuleInit {
  private readonly logger = new Logger(GpsPrinterService.name);

  constructor(private readonly kafka: KafkaService) {}

  async onModuleInit(): Promise<void> {
    const groupId = `${kafkaConfig.clientId}-gps-printer`;
    const consumer = await this.kafka.createConsumer(groupId);

    // Log rebalances so you can see which partitions this instance owns
    consumer.on(consumer.events.REBALANCING, () => {
      this.logger.warn(`[${groupId}] rebalancing…`);
    });

    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
      const assigned = payload.memberAssignment ?? {};
      const summary = Object.entries(assigned)
        .map(([topic, partitions]) => `${topic}=[${(partitions as number[]).join(', ')}]`)
        .join(' ');
      this.logger.log(
        `[${groupId}] joined — member=${payload.memberId} assignment: ${summary || '(none)'}`,
      );
    });

    await consumer.subscribe({
      topic: kafkaConfig.gpsEventsTopic,
      fromBeginning: true,
    });

    this.logger.log(`Listening on topic "${kafkaConfig.gpsEventsTopic}" (group=${groupId})`);

    await consumer.run({
      eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
        this.logger.log(
          `topic=${topic} partition=${partition} offset=${message.offset} key=${message.key?.toString()} value=${message.value?.toString()}`,
        );
      },
    });
  }
}
