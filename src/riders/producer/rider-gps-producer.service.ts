import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { KafkaService } from '../../shared/kafka/kafka.service';
import { SchemaRegistryService } from '../../shared/kafka/schema-registry.service';
import { kafkaConfig } from '../../shared/kafka/kafka.config';
import { RiderGpsEvent, RiderStatus } from '../../shared/kafka/rider-gps-event';

const STATUSES: RiderStatus[] = ['searching', 'waiting', 'on_trip'];

/** Rough Cairo bounding box for demo coordinates (same as driver producer) */
const LAT_MIN = 29.95;
const LAT_MAX = 30.15;
const LON_MIN = 31.15;
const LON_MAX = 31.45;

interface RiderState {
  id: string;
  latitude: number;
  longitude: number;
  status: RiderStatus;
  heading: number;
}

@Injectable()
export class RiderGpsProducerService implements OnModuleInit {
  private readonly logger = new Logger(RiderGpsProducerService.name);
  private riders: RiderState[] = [];
  private running = true;

  constructor(
    private readonly kafka: KafkaService,
    private readonly schemas: SchemaRegistryService,
  ) {}

  async onModuleInit(): Promise<void> {
    this.riders = this.seedRiders(kafkaConfig.riderCount);
    this.logger.log(
      `Starting rider GPS producer: ${this.riders.length} riders → topic "${kafkaConfig.gpsEventsRiderTopic}" (Avro)`,
    );

    const producer = await this.kafka.createProducer();

    process.on('SIGINT', () => {
      this.running = false;
    });
    process.on('SIGTERM', () => {
      this.running = false;
    });

    void this.emitLoop(producer);
  }

  private async emitLoop(
    producer: Awaited<ReturnType<KafkaService['createProducer']>>,
  ): Promise<void> {
    while (this.running) {
      for (const rider of this.riders) {
        this.nudge(rider);
        const event = this.toEvent(rider);
        const value = await this.schemas.encodeRider(event);
        const result = await producer.send({
          topic: kafkaConfig.gpsEventsRiderTopic,
          messages: [
            {
              // Partition by rider_id so events stay ordered per rider
              key: rider.id,
              value,
            },
          ],
        });

        const meta = result[0];
        this.logger.log(
          `sent rider=${rider.id} partition=${meta.partition} offset=${meta.baseOffset} status=${event.status} heading=${event.heading?.toFixed(0) ?? 'null'}`,
        );
      }

      await this.sleep(2000 + Math.floor(Math.random() * 3001));
    }
  }

  private seedRiders(count: number): RiderState[] {
    return Array.from({ length: count }, (_, i) => ({
      id: `rider-${String(i + 1).padStart(3, '0')}`,
      latitude: LAT_MIN + Math.random() * (LAT_MAX - LAT_MIN),
      longitude: LON_MIN + Math.random() * (LON_MAX - LON_MIN),
      status: STATUSES[Math.floor(Math.random() * STATUSES.length)],
      heading: Math.random() * 360,
    }));
  }

  private nudge(rider: RiderState): void {
    rider.latitude = this.clamp(
      rider.latitude + (Math.random() - 0.5) * 0.002,
      LAT_MIN,
      LAT_MAX,
    );
    rider.longitude = this.clamp(
      rider.longitude + (Math.random() - 0.5) * 0.002,
      LON_MIN,
      LON_MAX,
    );
    rider.heading = (rider.heading + (Math.random() - 0.5) * 20 + 360) % 360;
    if (Math.random() < 0.05) {
      rider.status = STATUSES[Math.floor(Math.random() * STATUSES.length)];
    }
  }

  private toEvent(rider: RiderState): RiderGpsEvent {
    return {
      rider_id: rider.id,
      latitude: rider.latitude,
      longitude: rider.longitude,
      timestamp: Date.now(),
      status: rider.status,
      heading: rider.heading,
    };
  }

  private clamp(value: number, min: number, max: number): number {
    return Math.max(min, Math.min(max, value));
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
