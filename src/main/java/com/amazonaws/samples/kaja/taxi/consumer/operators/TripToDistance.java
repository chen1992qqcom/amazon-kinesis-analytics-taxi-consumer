package com.amazonaws.samples.kaja.taxi.consumer.operators;

import ch.hsr.geohash.GeoHash;
import com.amazonaws.samples.kaja.taxi.consumer.events.flink.TripDistance;
import com.amazonaws.samples.kaja.taxi.consumer.events.flink.TripDuration;
import com.amazonaws.samples.kaja.taxi.consumer.events.kinesis.TripEvent;
import com.amazonaws.samples.kaja.taxi.consumer.utils.GeoUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class TripToDistance implements  FlatMapFunction<TripEvent, TripDistance> {
    @Override
    public void flatMap(TripEvent tripEvent, Collector<TripDistance> collector) {
        String pickupLocation = GeoHash.geoHashStringWithCharacterPrecision(tripEvent.pickupLatitude, tripEvent.pickupLongitude, 6);
        double distance = GeoUtils.getDistance(tripEvent.pickupLatitude, tripEvent.pickupLongitude, tripEvent.dropoffLatitude, tripEvent.dropoffLongitude);


        if (GeoUtils.nearJFK(tripEvent.dropoffLatitude, tripEvent.dropoffLongitude)) {
            collector.collect(new TripDistance(distance, pickupLocation, "JFK"));
        } else if (GeoUtils.nearLGA(tripEvent.dropoffLatitude, tripEvent.dropoffLongitude)) {
            collector.collect(new TripDistance(distance, pickupLocation, "LGA"));
        }

    }

}
