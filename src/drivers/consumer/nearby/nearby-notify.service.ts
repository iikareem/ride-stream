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
 * Consumes driver GPS, GEOSEARCHes nearby riders, PUBLISHes to user:{riderId}.
 */
@Injectable()
export class NearbyNotifyService implements OnModuleInit {
  private readonly logger = new Logger(NearbyNotifyService.name);

  constructor(
    private readonly kafka: KafkaService,
    private readonly schemas: SchemaRegistryService,
    private readonly redis: RedisService,
  ) {}

  async onModuleInit(): Promise<void> {
    const groupId = kafkaConfig.nearbyGroupId;
    const consumer = await this.kafka.createConsumer(groupId);
    attachRebalanceLogging(consumer, groupId, this.logger);

    await consumer.subscribe({
      topic: kafkaConfig.gpsEventsDriverTopic,
      fromBeginning: kafkaConfig.consumeFromBeginning,
    });

    this.logger.log(
      `Nearby listening on "${kafkaConfig.gpsEventsDriverTopic}" → GEOSEARCH "${redisConfig.ridersGeoKey}" radius=${redisConfig.nearbyRadiusKm}km → PUBLISH user:* (group=${groupId}, fromBeginning=${kafkaConfig.consumeFromBeginning})`,
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
        const riders = await this.redis.geoSearchByRadius(
          redisConfig.ridersGeoKey,
          event.longitude,
          event.latitude,
          redisConfig.nearbyRadiusKm,
        );

        const payload = {
          driver_id: event.driver_id,
          latitude: event.latitude,
          longitude: event.longitude,
          speed_kmh: event.speed_kmh,
          heading: event.heading,
          status: event.status,
          timestamp: event.timestamp,
        };

        for (const riderId of riders) {
          await this.redis.publishUser(riderId, payload);
        }

        const lag = latencyMs(event.timestamp);
        this.logger.log(
          `nearby driver=${event.driver_id} lat=${event.latitude.toFixed(5)} lon=${event.longitude.toFixed(5)} riders=${riders.length}${riders.length ? ` [${riders.join(',')}]` : ''} radius_km=${redisConfig.nearbyRadiusKm} latency_ms=${lag}`,
        );
      },
    });
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
