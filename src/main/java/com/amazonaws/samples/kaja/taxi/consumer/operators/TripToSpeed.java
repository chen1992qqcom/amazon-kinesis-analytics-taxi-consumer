package com.amazonaws.samples.kaja.taxi.consumer.operators;

import ch.hsr.geohash.GeoHash;
import com.amazonaws.samples.kaja.taxi.consumer.events.flink.TripDistance;
import com.amazonaws.samples.kaja.taxi.consumer.events.flink.TripSpeed;
import com.amazonaws.samples.kaja.taxi.consumer.events.kinesis.TripEvent;
import com.amazonaws.samples.kaja.taxi.consumer.utils.GeoUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class TripToSpeed implements FlatMapFunction<TripEvent, TripSpeed> {
    @Override
    public void flatMap(TripEvent tripEvent, Collector<TripSpeed> collector) {
        String pickupLocation = GeoHash.geoHashStringWithCharacterPrecision(tripEvent.pickupLatitude, tripEvent.pickupLongitude, 6);
        double distance = GeoUtils.getDistance(tripEvent.pickupLatitude, tripEvent.pickupLongitude, tripEvent.dropoffLatitude, tripEvent.dropoffLongitude);
        double tripDuration = Duration.between(tripEvent.pickupDatetime, tripEvent.dropoffDatetime).toMinutes();
        double speed = distance / tripDuration * 60; // km per hour


        if (GeoUtils.nearJFK(tripEvent.dropoffLatitude, tripEvent.dropoffLongitude)) {
            collector.collect(new TripSpeed(speed, pickupLocation, "JFK"));
        } else if (GeoUtils.nearLGA(tripEvent.dropoffLatitude, tripEvent.dropoffLongitude)) {
            collector.collect(new TripSpeed(speed, pickupLocation, "LGA"));
        }

    }

}
