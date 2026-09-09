import { Module } from '@nestjs/common';
import { KafkaModule } from '../../../shared/kafka/kafka.module';
import { RedisModule } from '../../../shared/redis/redis.module';
import { RiderGeoService } from './rider-geo.service';

@Module({
  imports: [KafkaModule, RedisModule],
  providers: [RiderGeoService],
})
export class RiderGeoModule {}
