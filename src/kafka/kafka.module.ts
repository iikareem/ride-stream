import { Module } from '@nestjs/common';
import { KafkaService } from './kafka.service';
import { SchemaRegistryService } from './schema-registry.service';

@Module({
  providers: [KafkaService, SchemaRegistryService],
  exports: [KafkaService, SchemaRegistryService],
})
export class KafkaModule {}
