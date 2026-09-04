import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { EachMessagePayload } from 'kafkajs';
import { KafkaService } from '../kafka/kafka.service';
import { SchemaRegistryService } from '../kafka/schema-registry.service';
import { kafkaConfig } from '../kafka/kafka.config';
import { GpsEvent } from '../kafka/gps-event';
import { EtaUpdate } from '../kafka/eta-update';
import {
  attachRebalanceLogging,
  latencyMs,
} from '../kafka/consumer-observability';

/** Cairo-ish box used to pick a stable fake destination per driver */
const DEST_LAT_MIN = 29.95;
const DEST_LAT_MAX = 30.15;
const DEST_LON_MIN = 31.15;
const DEST_LON_MAX = 31.45;

const MIN_SPEED_KMH = 5;

interface Destination {
  lat: number;
  lon: number;
}

@Injectable()
export class EtaCalculatorService implements OnModuleInit {
  private readonly logger = new Logger(EtaCalculatorService.name);
  private readonly destinations = new Map<string, Destination>();

  constructor(
    private readonly kafka: KafkaService,
    private readonly schemas: SchemaRegistryService,
  ) {}

  async onModuleInit(): Promise<void> {
    const groupId = kafkaConfig.etaGroupId;
    const consumer = await this.kafka.createConsumer(groupId);
    const producer = await this.kafka.createProducer();
    attachRebalanceLogging(consumer, groupId, this.logger);

    await consumer.subscribe({
      topic: kafkaConfig.gpsEventsTopic,
      fromBeginning: kafkaConfig.consumeFromBeginning,
    });

    this.logger.log(
      `ETA calculator listening on "${kafkaConfig.gpsEventsTopic}" → "${kafkaConfig.etaUpdatesTopic}" (group=${groupId}, fromBeginning=${kafkaConfig.consumeFromBeginning}, delayMs=${kafkaConfig.processingDelayMs})`,
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

        const gps = await this.schemas.decode(message.value);
        const update = this.toEtaUpdate(gps);
        const value = await this.schemas.encodeEta(update);

        const result = await producer.send({
          topic: kafkaConfig.etaUpdatesTopic,
          messages: [
            {
              key: update.driver_id,
              value,
            },
          ],
        });

        const meta = result[0];
        const lag = latencyMs(gps.timestamp);
        this.logger.log(
          `eta driver=${update.driver_id} distance_km=${update.distance_km.toFixed(2)} eta_s=${update.eta_seconds} latency_ms=${lag} → ${kafkaConfig.etaUpdatesTopic} p=${meta.partition} off=${meta.baseOffset}`,
        );
      },
    });
  }

  private toEtaUpdate(gps: GpsEvent): EtaUpdate {
    const dest = this.destinationFor(gps.driver_id);
    const distanceKm = this.haversineKm(
      gps.latitude,
      gps.longitude,
      dest.lat,
      dest.lon,
    );
    const speed = Math.max(Number(gps.speed_kmh), MIN_SPEED_KMH);
    const etaSeconds = Math.round((distanceKm / speed) * 3600);

    return {
      driver_id: gps.driver_id,
      latitude: gps.latitude,
      longitude: gps.longitude,
      destination_lat: dest.lat,
      destination_lon: dest.lon,
      distance_km: distanceKm,
      speed_kmh: speed,
      eta_seconds: etaSeconds,
      timestamp: gps.timestamp,
    };
  }

  /** Stable fake destination per driver (learning stand-in for a trip destination). */
  private destinationFor(driverId: string): Destination {
    const existing = this.destinations.get(driverId);
    if (existing) {
      return existing;
    }

    const hash = this.hashString(driverId);
    const dest: Destination = {
      lat: DEST_LAT_MIN + (hash % 1000) / 1000 * (DEST_LAT_MAX - DEST_LAT_MIN),
      lon:
        DEST_LON_MIN +
        ((hash >> 10) % 1000) / 1000 * (DEST_LON_MAX - DEST_LON_MIN),
    };
    this.destinations.set(driverId, dest);
    this.logger.log(
      `Assigned destination for ${driverId}: lat=${dest.lat.toFixed(5)} lon=${dest.lon.toFixed(5)}`,
    );
    return dest;
  }

  private haversineKm(
    lat1: number,
    lon1: number,
    lat2: number,
    lon2: number,
  ): number {
    const toRad = (deg: number) => (deg * Math.PI) / 180;
    const r = 6371;
    const dLat = toRad(lat2 - lat1);
    const dLon = toRad(lon2 - lon1);
    const a =
      Math.sin(dLat / 2) ** 2 +
      Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
    return 2 * r * Math.asin(Math.sqrt(a));
  }

  private hashString(value: string): number {
    let hash = 0;
    for (let i = 0; i < value.length; i += 1) {
      hash = (hash * 31 + value.charCodeAt(i)) >>> 0;
    }
    return hash;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
