import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { SchemaRegistry, SchemaType } from '@kafkajs/confluent-schema-registry';
import { kafkaConfig } from './kafka.config';
import { GpsEvent } from './gps-event';
import { EtaUpdate } from './eta-update';
import {
  GPS_EVENT_SCHEMA_V1,
  GPS_EVENT_SCHEMA_V2,
  GPS_EVENT_VALUE_SUBJECT,
} from './schemas/gps-event.avsc';
import {
  ETA_UPDATE_SCHEMA_V1,
  ETA_UPDATE_VALUE_SUBJECT,
} from './schemas/eta-update.avsc';

@Injectable()
export class SchemaRegistryService implements OnModuleInit {
  private readonly logger = new Logger(SchemaRegistryService.name);
  private readonly registry = new SchemaRegistry({
    host: kafkaConfig.schemaRegistryUrl,
  });

  private gpsSchemaId: number | null = null;
  private etaSchemaId: number | null = null;

  async onModuleInit(): Promise<void> {
    await this.ensureSchemasRegistered();
  }

  /**
   * Registers GPS v1 then v2, and EtaUpdate v1, so subjects exist before produce/consume.
   */
  async ensureSchemasRegistered(): Promise<void> {
    const gpsV1 = await this.registry.register(
      {
        type: SchemaType.AVRO,
        schema: JSON.stringify(GPS_EVENT_SCHEMA_V1),
      },
      { subject: GPS_EVENT_VALUE_SUBJECT },
    );
    this.logger.log(
      `Registered ${GPS_EVENT_VALUE_SUBJECT} v1 schema id=${gpsV1.id}`,
    );

    const gpsV2 = await this.registry.register(
      {
        type: SchemaType.AVRO,
        schema: JSON.stringify(GPS_EVENT_SCHEMA_V2),
      },
      { subject: GPS_EVENT_VALUE_SUBJECT },
    );
    this.gpsSchemaId = gpsV2.id;
    this.logger.log(
      `Registered ${GPS_EVENT_VALUE_SUBJECT} v2 schema id=${gpsV2.id} (optional heading)`,
    );

    const etaV1 = await this.registry.register(
      {
        type: SchemaType.AVRO,
        schema: JSON.stringify(ETA_UPDATE_SCHEMA_V1),
      },
      { subject: ETA_UPDATE_VALUE_SUBJECT },
    );
    this.etaSchemaId = etaV1.id;
    this.logger.log(
      `Registered ${ETA_UPDATE_VALUE_SUBJECT} v1 schema id=${etaV1.id}`,
    );
  }

  async encode(event: GpsEvent): Promise<Buffer> {
    if (this.gpsSchemaId === null) {
      await this.ensureSchemasRegistered();
    }
    return this.registry.encode(this.gpsSchemaId!, {
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
    return (await this.registry.decode(buffer)) as GpsEvent;
  }

  async encodeEta(update: EtaUpdate): Promise<Buffer> {
    if (this.etaSchemaId === null) {
      await this.ensureSchemasRegistered();
    }
    return this.registry.encode(this.etaSchemaId!, {
      driver_id: update.driver_id,
      latitude: update.latitude,
      longitude: update.longitude,
      destination_lat: update.destination_lat,
      destination_lon: update.destination_lon,
      distance_km: update.distance_km,
      speed_kmh: update.speed_kmh,
      eta_seconds: update.eta_seconds,
      timestamp: update.timestamp,
    });
  }

  async decodeEta(buffer: Buffer): Promise<EtaUpdate> {
    return (await this.registry.decode(buffer)) as EtaUpdate;
  }
}
