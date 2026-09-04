import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { EachMessagePayload } from 'kafkajs';
import { KafkaService } from '../kafka/kafka.service';
import { SchemaRegistryService } from '../kafka/schema-registry.service';
import { kafkaConfig } from '../kafka/kafka.config';
import {
  attachRebalanceLogging,
  latencyMs,
} from '../kafka/consumer-observability';

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
    attachRebalanceLogging(consumer, groupId, this.logger);

    await consumer.subscribe({
      topic: kafkaConfig.gpsEventsTopic,
      fromBeginning: kafkaConfig.consumeFromBeginning,
    });

    this.logger.log(
      `Listening on topic "${kafkaConfig.gpsEventsTopic}" (group=${groupId}, Avro, fromBeginning=${kafkaConfig.consumeFromBeginning}, delayMs=${kafkaConfig.processingDelayMs})`,
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

        const event = await this.schemas.decode(message.value);
        const lag = latencyMs(event.timestamp);
        this.logger.log(
          `topic=${topic} partition=${partition} offset=${message.offset} key=${message.key?.toString()} driver=${event.driver_id} lat=${event.latitude.toFixed(5)} lon=${event.longitude.toFixed(5)} speed=${Number(event.speed_kmh).toFixed(1)} heading=${event.heading ?? 'null'} status=${event.status} latency_ms=${lag}`,
        );
      },
    });
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
