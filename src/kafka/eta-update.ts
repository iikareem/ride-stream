export interface EtaUpdate {
  driver_id: string;
  latitude: number;
  longitude: number;
  destination_lat: number;
  destination_lon: number;
  distance_km: number;
  speed_kmh: number;
  eta_seconds: number;
  timestamp: number;
}
