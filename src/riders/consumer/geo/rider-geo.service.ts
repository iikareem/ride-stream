import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { EachMessagePayload } from 'kafkajs';
import { KafkaService } from '../../../shared/kafka/kafka.service';
import { SchemaRegistryService } from '../../../shared/kafka/schema-registry.service';
import { kafkaConfig } from '../../../shared/kafka/kafka.config';
import { RedisService } from '../../../shared/redis/redis.service';
import { redisConfig } from '../../../shared/redis/redis.config';
import {
  attachRebalanceLogging,
  latencyMs,
} from '../../../shared/kafka/consumer-observability';

/**
 * Consumes rider GPS and upserts each rider into a Redis GEO index
 * so later workers can GEOSEARCH nearby riders around a driver.
 */
@Injectable()
export class RiderGeoService implements OnModuleInit {
  private readonly logger = new Logger(RiderGeoService.name);

  constructor(
    private readonly kafka: KafkaService,
    private readonly schemas: SchemaRegistryService,
    private readonly redis: RedisService,
  ) {}

  async onModuleInit(): Promise<void> {
    const groupId = kafkaConfig.riderGeoGroupId;
    const consumer = await this.kafka.createConsumer(groupId);
    attachRebalanceLogging(consumer, groupId, this.logger);

    await consumer.subscribe({
      topic: kafkaConfig.gpsEventsRiderTopic,
      fromBeginning: kafkaConfig.consumeFromBeginning,
    });

    this.logger.log(
      `Rider GEO listening on "${kafkaConfig.gpsEventsRiderTopic}" → Redis GEO "${redisConfig.ridersGeoKey}" (group=${groupId}, fromBeginning=${kafkaConfig.consumeFromBeginning})`,
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

        const event = await this.schemas.decodeRider(message.value);

        // Redis GEOADD: longitude then latitude
        await this.redis.geoAdd(
          redisConfig.ridersGeoKey,
          event.longitude,
          event.latitude,
          event.rider_id,
        );

        const lag = latencyMs(event.timestamp);
        this.logger.log(
          `geoadd rider=${event.rider_id} lat=${event.latitude.toFixed(5)} lon=${event.longitude.toFixed(5)} status=${event.status} key=${redisConfig.ridersGeoKey} latency_ms=${lag}`,
        );
      },
    });
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
