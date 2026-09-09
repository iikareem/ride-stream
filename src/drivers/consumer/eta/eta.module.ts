import { Module } from '@nestjs/common';
import { KafkaModule } from '../../../shared/kafka/kafka.module';
import { EtaCalculatorService } from './eta-calculator.service';

@Module({
  imports: [KafkaModule],
  providers: [EtaCalculatorService],
})
export class EtaModule {}
