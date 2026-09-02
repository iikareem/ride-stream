import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { GpsPrinterService } from './gps-printer.service';

@Module({
  imports: [KafkaModule],
  providers: [GpsPrinterService],
})
export class ConsumerModule {}
