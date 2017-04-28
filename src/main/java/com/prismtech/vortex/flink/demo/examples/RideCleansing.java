package com.prismtech.vortex.flink.demo.examples;

import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.prismtech.vortex.flink.FlinkVortexReader;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.omg.dds.topic.Topic;
import vortex.flinkdemo.TaxiRide;

import static vortex.commons.util.VConfig.DefaultEntities.defaultDomainParticipant;
import static vortex.commons.util.VConfig.DefaultEntities.defaultPolicyFactory;

/**
 * Based on http://dataartisans.github.io/flink-training
 *
 * Filter the data stream of taxi ride records coming from DDS.
 *
 * Keep only rides that start and end within New York City. The resulting stream is printed.
 */
public class RideCleansing {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        // Set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start reading data from DDS
        final Topic<TaxiRide> taxiRideTopic =
                defaultDomainParticipant().createTopic("TaxiRide", TaxiRide.class);
        DataStream<TaxiRide> rides = env.addSource(
                new FlinkVortexReader<TaxiRide>(taxiRideTopic, params.getProperties(),
                        defaultPolicyFactory().Durability().withTransientLocal(),
                        defaultPolicyFactory().Reliability().withReliable()));

        DataStream<TaxiRide> filteredRides = rides
                // filter out rides that do start or stop in NYC
                .filter(new NYCFilter());

        // print the filtered stream
        filteredRides.addSink(new SinkFunction<TaxiRide>() {
            @Override
            public void invoke(TaxiRide value) throws Exception {
                System.out.println(RideCleansing.toString(value));
            }
        });

        // run the cleansing pipeline
        env.execute("Taxi Ride Cleansing");
    }

    public static class NYCFilter implements FilterFunction<TaxiRide> {

        @Override
        public boolean filter(TaxiRide ride) throws Exception {
            return GeoUtils.isInNYC(ride.startLon, ride.startLat) &&
                    GeoUtils.isInNYC(ride.endLon, ride.endLat);
        }
    }

    public static String toString(TaxiRide ride) {
        StringBuilder sb = new StringBuilder();
        sb.append(ride.rideId).append(",");
        sb.append(ride.isStart ? "START" : "END").append(",");
        if (ride.isStart) {
            sb.append(ride.startTime).append(",");
            sb.append(ride.endTime).append(",");
        } else {
            sb.append(ride.endTime).append(",");
            sb.append(ride.startTime).append(",");
        }
        sb.append(ride.startLon).append(",");
        sb.append(ride.startLat).append(",");
        sb.append(ride.endLon).append(",");
        sb.append(ride.endLat).append(",");
        sb.append(ride.passengerCnt);

        return sb.toString();
    }
}
