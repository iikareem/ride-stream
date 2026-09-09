export type RiderStatus = 'searching' | 'waiting' | 'on_trip';

export interface RiderGpsEvent {
  rider_id: string;
  latitude: number;
  longitude: number;
  timestamp: number;
  status: RiderStatus;
  /** Degrees 0–360; null/undefined when unknown */
  heading?: number | null;
}
