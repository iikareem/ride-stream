import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { SchemaRegistry, SchemaType } from '@kafkajs/confluent-schema-registry';
import { kafkaConfig } from './kafka.config';
import { GpsEvent } from './gps-event';
import {
  GPS_EVENT_SCHEMA_V1,
  GPS_EVENT_SCHEMA_V2,
  GPS_EVENT_VALUE_SUBJECT,
} from './schemas/gps-event.avsc';

@Injectable()
export class SchemaRegistryService implements OnModuleInit {
  private readonly logger = new Logger(SchemaRegistryService.name);
  private readonly registry = new SchemaRegistry({
    host: kafkaConfig.schemaRegistryUrl,
  });

  private schemaId: number | null = null;

  async onModuleInit(): Promise<void> {
    await this.ensureSchemasRegistered();
  }

  /**
   * Registers v1 then v2 under the same subject so Schema Registry retains
   * version history and enforces BACKWARD compatibility on the evolution.
   */
  async ensureSchemasRegistered(): Promise<void> {
    const v1 = await this.registry.register(
      {
        type: SchemaType.AVRO,
        schema: JSON.stringify(GPS_EVENT_SCHEMA_V1),
      },
      { subject: GPS_EVENT_VALUE_SUBJECT },
    );
    this.logger.log(
      `Registered ${GPS_EVENT_VALUE_SUBJECT} v1 schema id=${v1.id}`,
    );

    const v2 = await this.registry.register(
      {
        type: SchemaType.AVRO,
        schema: JSON.stringify(GPS_EVENT_SCHEMA_V2),
      },
      { subject: GPS_EVENT_VALUE_SUBJECT },
    );
    this.schemaId = v2.id;
    this.logger.log(
      `Registered ${GPS_EVENT_VALUE_SUBJECT} v2 schema id=${v2.id} (optional heading)`,
    );
  }

  async encode(event: GpsEvent): Promise<Buffer> {
    if (this.schemaId === null) {
      await this.ensureSchemasRegistered();
    }
    return this.registry.encode(this.schemaId!, {
      driver_id: event.driver_id,
      latitude: event.latitude,
      longitude: event.longitude,
      speed_kmh: event.speed_kmh,
      timestamp: event.timestamp,
      status: event.status,
      heading: event.heading ?? null,
    });
  }

  async decode(buffer: Buffer): Promise<GpsEvent> {
    const decoded = (await this.registry.decode(buffer)) as GpsEvent;
    return decoded;
  }
}
