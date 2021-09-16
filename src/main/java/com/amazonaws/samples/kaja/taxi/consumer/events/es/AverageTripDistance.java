package com.amazonaws.samples.kaja.taxi.consumer.events.es;

public class AverageTripDistance extends Document {
    public final String location;
    public final String airportCode;
    public final double avgTravelDistance;
    public final double sumTravelDistance;
    public AverageTripDistance(String location, String airportCode, double avgTravelDistance, double sumTravelDistance, long timestamp) {
        super(timestamp);

        this.location = location;
        this.airportCode = airportCode;
        this.avgTravelDistance = avgTravelDistance;
        this.sumTravelDistance = sumTravelDistance;
    }
}
