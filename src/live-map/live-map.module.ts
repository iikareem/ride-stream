import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { LiveMapUpdaterService } from './live-map-updater.service';

@Module({
  imports: [KafkaModule],
  providers: [LiveMapUpdaterService],
})
export class LiveMapModule {}
