import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { EachMessagePayload } from 'kafkajs';
import { KafkaService } from '../kafka/kafka.service';
import { SchemaRegistryService } from '../kafka/schema-registry.service';
import { kafkaConfig } from '../kafka/kafka.config';

@Injectable()
export class GpsPrinterService implements OnModuleInit {
  private readonly logger = new Logger(GpsPrinterService.name);

  constructor(
    private readonly kafka: KafkaService,
    private readonly schemas: SchemaRegistryService,
  ) {}

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

    this.logger.log(
      `Listening on topic "${kafkaConfig.gpsEventsTopic}" (group=${groupId}, Avro)`,
    );

    await consumer.run({
      eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
        if (!message.value) {
          this.logger.warn(
            `topic=${topic} partition=${partition} offset=${message.offset} empty value`,
          );
          return;
        }

        const event = await this.schemas.decode(message.value);
        this.logger.log(
          `topic=${topic} partition=${partition} offset=${message.offset} key=${message.key?.toString()} driver=${event.driver_id} lat=${event.latitude.toFixed(5)} lon=${event.longitude.toFixed(5)} speed=${Number(event.speed_kmh).toFixed(1)} heading=${event.heading ?? 'null'} status=${event.status}`,
        );
      },
    });
  }
}
