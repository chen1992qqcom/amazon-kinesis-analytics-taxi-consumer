package com.amazonaws.samples.kaja.taxi.consumer.operators;

import com.amazonaws.samples.kaja.taxi.consumer.events.es.AverageTripSpeed;
import com.amazonaws.samples.kaja.taxi.consumer.events.flink.TripSpeed;
import com.google.common.collect.Iterables;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.stream.StreamSupport;

public class TripSpeedToAverageTripSpeed implements WindowFunction<TripSpeed, AverageTripSpeed, Tuple2<String, String>, TimeWindow> {


    @Override
    public void apply(Tuple2<String, String> tuple, TimeWindow timeWindow, Iterable<TripSpeed> iterable, Collector<AverageTripSpeed> collector) {
        if (Iterables.size(iterable) > 1) {
            String location = Iterables.get(iterable, 0).pickupGeoHash;
            String airportCode = Iterables.get(iterable, 0).airportCode;

            double sumSpeed = StreamSupport
                    .stream(iterable.spliterator(), false)
                    .mapToDouble(trip -> trip.speed)
                    .sum();

            double avgSpeed = sumSpeed / Iterables.size(iterable);

            collector.collect(new AverageTripSpeed(location, airportCode, avgSpeed, timeWindow.getEnd()));
        }


    }
}
