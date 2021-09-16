package com.amazonaws.samples.kaja.taxi.consumer.events.flink;

public class TripDistance {
    public final String airportCode;
    public final String pickupGeoHash;
    public final double distance;


    public TripDistance(double distance, String pickupGeoHash, String airportCode) {
        this.pickupGeoHash = pickupGeoHash;
        this.airportCode = airportCode;
        this.distance = distance;
    }
}
