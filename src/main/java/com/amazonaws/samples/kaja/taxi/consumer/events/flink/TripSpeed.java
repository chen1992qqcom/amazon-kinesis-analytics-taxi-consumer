package com.amazonaws.samples.kaja.taxi.consumer.events.flink;

public class TripSpeed {
    public final String airportCode;
    public final String pickupGeoHash;
    public final double speed;

    public TripSpeed(double speed, String pickupGeoHash, String airportCode) {
        this.pickupGeoHash = pickupGeoHash;
        this.airportCode = airportCode;
        this.speed = speed;
    }
}
