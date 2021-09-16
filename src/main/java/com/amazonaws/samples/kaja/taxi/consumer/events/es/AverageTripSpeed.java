package com.amazonaws.samples.kaja.taxi.consumer.events.es;

public class AverageTripSpeed extends Document {
    public final String location;
    public final String airportCode;
    public final double avgTravelSpeed;

    public AverageTripSpeed(String location, String airportCode, double avgTravelSpeed, long timestamp) {
        super(timestamp);

        this.location = location;
        this.airportCode = airportCode;
        this.avgTravelSpeed = avgTravelSpeed;
    }
}
