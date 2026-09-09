import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { RedisModule } from '../redis/redis.module';
import { RiderGeoService } from './rider-geo.service';

@Module({
  imports: [KafkaModule, RedisModule],
  providers: [RiderGeoService],
})
export class RiderGeoModule {}
