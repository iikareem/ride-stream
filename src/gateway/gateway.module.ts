import { Module } from '@nestjs/common';
import { RedisModule } from '../redis/redis.module';
import { EventsGateway } from './events.gateway';

@Module({
  imports: [RedisModule],
  providers: [EventsGateway],
})
export class GatewayModule {}
