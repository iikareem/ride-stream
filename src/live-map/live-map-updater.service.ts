import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { EachMessagePayload } from 'kafkajs';
import { KafkaService } from '../kafka/kafka.service';
import { SchemaRegistryService } from '../kafka/schema-registry.service';
import { kafkaConfig } from '../kafka/kafka.config';
import { EtaUpdate } from '../kafka/eta-update';
import {
  attachRebalanceLogging,
  latencyMs,
} from '../kafka/consumer-observability';

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
    attachRebalanceLogging(consumer, groupId, this.logger);

    await consumer.subscribe({
      topic: kafkaConfig.etaUpdatesTopic,
      fromBeginning: kafkaConfig.consumeFromBeginning,
    });

    this.logger.log(
      `Live map updater listening on "${kafkaConfig.etaUpdatesTopic}" (group=${groupId}, fromBeginning=${kafkaConfig.consumeFromBeginning}, delayMs=${kafkaConfig.processingDelayMs})`,
    );

    await consumer.run({
      eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
        if (!message.value) {
          this.logger.warn(
            `topic=${topic} partition=${partition} offset=${message.offset} empty value`,
          );
          return;
        }

        if (kafkaConfig.processingDelayMs > 0) {
          await sleep(kafkaConfig.processingDelayMs);
        }

        const update = await this.schemas.decodeEta(message.value);
        const isNew = !this.latest.has(update.driver_id);
        this.latest.set(update.driver_id, update);

        const lag = latencyMs(update.timestamp);
        this.logger.log(
          `map ${isNew ? 'add' : 'upd'} driver=${update.driver_id} lat=${update.latitude.toFixed(5)} lon=${update.longitude.toFixed(5)} eta_s=${update.eta_seconds} latency_ms=${lag} drivers=${this.latest.size}`,
        );
      },
    });
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
