import { Logger } from '@nestjs/common';
import { Consumer } from 'kafkajs';

/**
 * Attach GROUP_JOIN / REBALANCING / CRASH logs so you can watch partition ownership
 * change when members join or leave the same groupId.
 */
export function attachRebalanceLogging(
  consumer: Consumer,
  groupId: string,
  logger: Logger,
): void {
  consumer.on(consumer.events.REBALANCING, () => {
    logger.warn(`[${groupId}] rebalancing — partitions being revoked/reassigned`);
  });

  consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
    const assigned = payload.memberAssignment ?? {};
    const summary = Object.entries(assigned)
      .map(
        ([topic, partitions]) =>
          `${topic}=[${(partitions as number[]).join(', ')}]`,
      )
      .join(' ');
    logger.log(
      `[${groupId}] joined — member=${payload.memberId} leader=${payload.isLeader ? 'yes' : 'no'} assignment: ${summary || '(none)'}`,
    );
  });

  consumer.on(consumer.events.CRASH, ({ payload }) => {
    logger.error(
      `[${groupId}] crash — ${payload.error?.message ?? 'unknown'} restart=${payload.restart}`,
    );
  });

  consumer.on(consumer.events.DISCONNECT, () => {
    logger.warn(`[${groupId}] disconnected from broker`);
  });
}

/** Milliseconds since event/payload timestamp (producer wall clock). */
export function latencyMs(eventTimestamp: number, now = Date.now()): number {
  return Math.max(0, now - eventTimestamp);
}
