import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { GpsProducerService } from './gps-producer.service';

@Module({
  imports: [KafkaModule],
  providers: [GpsProducerService],
})
export class ProducerModule {}
