import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { KafkaService } from '../kafka/kafka.service';
import { SchemaRegistryService } from '../kafka/schema-registry.service';
import { kafkaConfig } from '../kafka/kafka.config';
import { DriverStatus, GpsEvent } from '../kafka/gps-event';

const STATUSES: DriverStatus[] = ['available', 'en_route', 'on_trip'];

/** Rough Cairo bounding box for demo coordinates */
const LAT_MIN = 29.95;
const LAT_MAX = 30.15;
const LON_MIN = 31.15;
const LON_MAX = 31.45;

interface DriverState {
  id: string;
  latitude: number;
  longitude: number;
  status: DriverStatus;
  heading: number;
}

@Injectable()
export class GpsProducerService implements OnModuleInit {
  private readonly logger = new Logger(GpsProducerService.name);
  private drivers: DriverState[] = [];
  private running = true;

  constructor(
    private readonly kafka: KafkaService,
    private readonly schemas: SchemaRegistryService,
  ) {}

  async onModuleInit(): Promise<void> {
    this.drivers = this.seedDrivers(kafkaConfig.driverCount);
    this.logger.log(
      `Starting GPS producer: ${this.drivers.length} drivers → topic "${kafkaConfig.gpsEventsTopic}" (Avro)`,
    );

    const producer = await this.kafka.createProducer();

    process.on('SIGINT', () => {
      this.running = false;
    });
    process.on('SIGTERM', () => {
      this.running = false;
    });

    // Don't block Nest bootstrap — run the emit loop in the background
    void this.emitLoop(producer);
  }

  private async emitLoop(
    producer: Awaited<ReturnType<KafkaService['createProducer']>>,
  ): Promise<void> {
    while (this.running) {
      for (const driver of this.drivers) {
        this.nudge(driver);
        const event = this.toEvent(driver);
        const value = await this.schemas.encode(event);
        const result = await producer.send({
          topic: kafkaConfig.gpsEventsTopic,
          messages: [
            {
              // Partition by driver_id so events stay ordered per driver
              key: driver.id,
              value,
            },
          ],
        });

        const meta = result[0];
        this.logger.log(
          `sent driver=${driver.id} partition=${meta.partition} offset=${meta.baseOffset} speed=${event.speed_kmh.toFixed(1)} heading=${event.heading?.toFixed(0) ?? 'null'}`,
        );
      }

      await this.sleep(2000 + Math.floor(Math.random() * 3001));
    }
  }

  private seedDrivers(count: number): DriverState[] {
    return Array.from({ length: count }, (_, i) => ({
      id: `driver-${String(i + 1).padStart(3, '0')}`,
      latitude: LAT_MIN + Math.random() * (LAT_MAX - LAT_MIN),
      longitude: LON_MIN + Math.random() * (LON_MAX - LON_MIN),
      status: STATUSES[Math.floor(Math.random() * STATUSES.length)],
      heading: Math.random() * 360,
    }));
  }

  private nudge(driver: DriverState): void {
    driver.latitude = this.clamp(
      driver.latitude + (Math.random() - 0.5) * 0.002,
      LAT_MIN,
      LAT_MAX,
    );
    driver.longitude = this.clamp(
      driver.longitude + (Math.random() - 0.5) * 0.002,
      LON_MIN,
      LON_MAX,
    );
    driver.heading = (driver.heading + (Math.random() - 0.5) * 20 + 360) % 360;
    if (Math.random() < 0.05) {
      driver.status = STATUSES[Math.floor(Math.random() * STATUSES.length)];
    }
  }

  private toEvent(driver: DriverState): GpsEvent {
    return {
      driver_id: driver.id,
      latitude: driver.latitude,
      longitude: driver.longitude,
      speed_kmh: 5 + Math.random() * 55,
      timestamp: Date.now(),
      status: driver.status,
      heading: driver.heading,
    };
  }

  private clamp(value: number, min: number, max: number): number {
    return Math.max(min, Math.min(max, value));
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
