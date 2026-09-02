import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { EachMessagePayload } from 'kafkajs';
import { KafkaService } from '../kafka/kafka.service';
import { kafkaConfig } from '../kafka/kafka.config';

@Injectable()
export class GpsPrinterService implements OnModuleInit {
  private readonly logger = new Logger(GpsPrinterService.name);

  constructor(private readonly kafka: KafkaService) {}

  async onModuleInit(): Promise<void> {
    const consumer = await this.kafka.createConsumer(
      `${kafkaConfig.clientId}-gps-printer`,
    );

    await consumer.subscribe({
      topic: kafkaConfig.gpsEventsTopic,
      fromBeginning: true,
    });

    this.logger.log(`Listening on topic "${kafkaConfig.gpsEventsTopic}"`);

    await consumer.run({
      eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
        this.logger.log(  
          `topic=${topic} partition=${partition} offset=${message.offset} key=${message.key?.toString()} value=${message.value?.toString()}`,
        );
      },
    });
  }
}
