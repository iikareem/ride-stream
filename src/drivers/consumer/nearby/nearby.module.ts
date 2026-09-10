import { Module } from '@nestjs/common';
import { KafkaModule } from '../../../shared/kafka/kafka.module';
import { RedisModule } from '../../../shared/redis/redis.module';
import { NearbyNotifyService } from './nearby-notify.service';

@Module({
  imports: [KafkaModule, RedisModule],
  providers: [NearbyNotifyService],
})
export class NearbyModule {}
