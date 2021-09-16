package com.amazonaws.samples.kaja.taxi.consumer.operators;

import com.amazonaws.samples.kaja.taxi.consumer.events.es.AverageTripDistance;
import com.amazonaws.samples.kaja.taxi.consumer.events.es.AverageTripDuration;
import com.amazonaws.samples.kaja.taxi.consumer.events.flink.TripDistance;
import com.google.common.collect.Iterables;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.stream.StreamSupport;

public class TripDistanceToAverageTripDistance implements WindowFunction<TripDistance, AverageTripDistance, Tuple2<String, String>, TimeWindow> {


    @Override
    public void apply(Tuple2<String, String> tuple, TimeWindow timeWindow, Iterable<TripDistance> iterable, Collector<AverageTripDistance> collector) {
        if (Iterables.size(iterable) > 1) {
            String location = Iterables.get(iterable, 0).pickupGeoHash;
            String airportCode = Iterables.get(iterable, 0).airportCode;

            double sumDistance = StreamSupport
                    .stream(iterable.spliterator(), false)
                    .mapToDouble(trip -> trip.distance)
                    .sum();

            double avgDistance = sumDistance / Iterables.size(iterable);

            collector.collect(new AverageTripDistance(location, airportCode, avgDistance, sumDistance, timeWindow.getEnd()));
        }


    }
}
