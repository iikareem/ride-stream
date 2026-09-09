import {
  ConnectedSocket,
  MessageBody,
  OnGatewayConnection,
  OnGatewayDisconnect,
  SubscribeMessage,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { Logger } from '@nestjs/common';
import { Server, Socket } from 'socket.io';

type JoinPayload = {
  userId?: string;
};

/**
 * Thin Socket.IO gateway — connect + join user:{id} room.
 * Delivery comes via Redis Socket.IO adapter (e.g. redis-emitter from consumers).
 * No business Redis Pub/Sub subscribe here.
 */
@WebSocketGateway({
  cors: { origin: '*' },
})
export class EventsGateway
  implements OnGatewayConnection, OnGatewayDisconnect
{
  private readonly logger = new Logger(EventsGateway.name);

  @WebSocketServer()
  server!: Server;

  handleConnection(client: Socket): void {
    this.logger.log(`connected socketId=${client.id}`);

    client.onAnyOutgoing((event, ...args) => {
      this.logger.log(
        `deliver socketId=${client.id} userId=${client.data.userId ?? 'none'} event=${event} payload=${safeJson(args[0])}`,
      );
    });
  }

  handleDisconnect(client: Socket): void {
    const userId = client.data.userId as string | undefined;
    this.logger.log(
      `disconnected socketId=${client.id} userId=${userId ?? 'none'}`,
    );
  }

  @SubscribeMessage('join')
  handleJoin(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: JoinPayload,
  ): { ok: boolean; room?: string; error?: string } {
    const userId = body?.userId?.trim();
    if (!userId) {
      return { ok: false, error: 'userId is required' };
    }

    const room = `user:${userId}`;
    void client.join(room);
    client.data.userId = userId;

    this.logger.log(
      `join socketId=${client.id} userId=${userId} room=${room}`,
    );

    return { ok: true, room };
  }
}

function safeJson(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}
