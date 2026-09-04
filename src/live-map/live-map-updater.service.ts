import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { EachMessagePayload } from 'kafkajs';
import { KafkaService } from '../kafka/kafka.service';
import { SchemaRegistryService } from '../kafka/schema-registry.service';
import { kafkaConfig } from '../kafka/kafka.config';
import { EtaUpdate } from '../kafka/eta-update';

@Injectable()
export class LiveMapUpdaterService implements OnModuleInit {
  private readonly logger = new Logger(LiveMapUpdaterService.name);
  /** Latest position + ETA per driver — learning stand-in for Redis later */
  private readonly latest = new Map<string, EtaUpdate>();

  constructor(
    private readonly kafka: KafkaService,
    private readonly schemas: SchemaRegistryService,
  ) {}

  async onModuleInit(): Promise<void> {
    const groupId = kafkaConfig.liveMapGroupId;
    const consumer = await this.kafka.createConsumer(groupId);

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
      topic: kafkaConfig.etaUpdatesTopic,
      fromBeginning: true,
    });

    this.logger.log(
      `Live map updater listening on "${kafkaConfig.etaUpdatesTopic}" (group=${groupId})`,
    );

    await consumer.run({
      eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
        if (!message.value) {
          this.logger.warn(
            `topic=${topic} partition=${partition} offset=${message.offset} empty value`,
          );
          return;
        }

        const update = await this.schemas.decodeEta(message.value);
        const isNew = !this.latest.has(update.driver_id);
        this.latest.set(update.driver_id, update);
        console.log(update.driver_id);

        this.logger.log(
          `map ${isNew ? 'add' : 'upd'} driver=${update.driver_id} lat=${update.latitude.toFixed(5)} lon=${update.longitude.toFixed(5)} eta_s=${update.eta_seconds} drivers=${this.latest.size}`,
        );
      },
    });
  }
}
