export type DriverStatus = 'available' | 'en_route' | 'on_trip';

export interface GpsEvent {
  driver_id: string;
  latitude: number;
  longitude: number;
  speed_kmh: number;
  timestamp: number;
  status: DriverStatus;
}
