import { Module } from '@nestjs/common';
import { KafkaModule } from './kafka/kafka.module';

/** Default Nest entry — Phase 1 apps use producer/main and consumer/main. */
@Module({
  imports: [KafkaModule],
})
export class AppModule {}
