import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { RiderGpsProducerService } from './rider-gps-producer.service';

@Module({
  imports: [KafkaModule],
  providers: [RiderGpsProducerService],
})
export class RiderProducerModule {}
