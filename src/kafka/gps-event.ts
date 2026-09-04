export type DriverStatus = 'available' | 'en_route' | 'on_trip';

export interface GpsEvent {
  driver_id: string;
  latitude: number;
  longitude: number;
  speed_kmh: number;
  timestamp: number;
  status: DriverStatus;
  /** Degrees 0–360; null/undefined when unknown (schema v2 optional field) */
  heading?: number | null;
}
