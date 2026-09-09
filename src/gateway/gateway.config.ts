export const gatewayConfig = {
  /** HTTP + Socket.IO listen port (Grafana uses 3000). */
  port: Number(process.env.GATEWAY_PORT ?? '3001'),
} as const;
