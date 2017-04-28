package com.prismtech.vortex.flink.demo.publisher;

import org.apache.commons.cli.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.omg.dds.core.policy.Durability;
import vortex.commons.util.Idioms;
import vortex.flinkdemo.TaxiRide;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPInputStream;

/**
 * Created by Vortex.
 */
public class TaxiRidePublisher implements Runnable{

    private final int maxDelayMsecs;
    private final int watermarkDelayMSecs;

    private final String dataFilePath;
    private final int servingSpeed;

    private transient  Idioms.Event<TaxiRide> taxiRideEvent;

    private transient BufferedReader reader;
    private transient InputStream gzipStream;


    public TaxiRidePublisher(String dataFilePath) {
        this(dataFilePath, 0, 1);
    }

    public TaxiRidePublisher(String dataFilePath, int servingSpeedFactor) {
        this(dataFilePath, 0, servingSpeedFactor);
    }

    public TaxiRidePublisher(String dataFilePath, int maxDelayMsecs, int servingSpeedFactor) {
        if(maxDelayMsecs < 0){
            throw new IllegalArgumentException("Max event delay must be positive");
        }

        this.maxDelayMsecs = maxDelayMsecs * 1000;
        this.watermarkDelayMSecs = maxDelayMsecs < 10000 ? 10000 : maxDelayMsecs;
        this.dataFilePath = dataFilePath;
        this.servingSpeed = servingSpeedFactor;


    }

    @Override
    public void run() {
        try {
            taxiRideEvent = new Idioms.Event<TaxiRide>("TaxiRide", TaxiRide.class,
                    Durability.Kind.TRANSIENT_LOCAL);
            gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
            reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));

            generateUnorderedStream();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {

                try {
                    if(reader != null) {
                        reader.close();
                        reader = null;
                    }
                    if(gzipStream != null) {
                        gzipStream.close();
                        gzipStream = null;
                    }
                } catch (IOException e) {
                }
        }
    }

    private void generateUnorderedStream() throws IOException, InterruptedException {
        long serviceStartTime = Calendar.getInstance().getTimeInMillis();
        long dataStartTime;

        Random rand = new Random(7452);
        PriorityQueue<Tuple2<Long, Object>> emitSchedule = new PriorityQueue<>(
                32,
                new Comparator<Tuple2<Long, Object>>() {
                    @Override
                    public int compare(Tuple2<Long, Object> o1, Tuple2<Long, Object> o2) {
                        return o1.f0.compareTo(o2.f0);
                    }
                }
        );

        // read first ride and insert it into emit schedule
        String line;
        TaxiRide ride;

        if(reader.ready() && (line = reader.readLine()) != null) {
            // read first ride
            ride = taxiRideFromString(line);
            // extract starting timestam
            dataStartTime = getEventTime(ride);
            // get delayed time
            long delayedEventTime = dataStartTime + getNormalDelayMsecs(rand);

            emitSchedule.add(new Tuple2<>(delayedEventTime, ride));
            // schedule next watermark
            long watermarkTime = dataStartTime + watermarkDelayMSecs;
            Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs -1 );
            emitSchedule.add(new Tuple2<>(watermarkTime, nextWatermark));
        } else {
            return;
        }

        // peek at next ride
        if(reader.ready() && (line = reader.readLine()) != null) {
            ride = taxiRideFromString(line);
        }

        while(emitSchedule.size() > 0 || reader.ready()) {
            // insert all events into schedule that might be emitted next
            long curNextDelayedEventTime = !emitSchedule.isEmpty() ? emitSchedule.peek().f0 : -1;
            long rideEventTime = ride != null ? getEventTime(ride) : -1;

            while(
                    ride != null && ( // while there is a ride AND
                            emitSchedule.isEmpty() || // and no ride in schedule OR
                            rideEventTime < curNextDelayedEventTime + maxDelayMsecs) // not enought rides in schedule
                    )
            {
                // insert event into emit schedule
                long delayedEventTime = rideEventTime + getNormalDelayMsecs(rand);
                emitSchedule.add(new Tuple2<>(delayedEventTime, ride));

                // read next ride
                if(reader.ready() && (line = reader.readLine()) != null) {
                    ride = taxiRideFromString(line);
                    rideEventTime = getEventTime(ride);
                } else {
                    ride = null;
                    rideEventTime = -1;
                }
            }

            // emit schedule is updated, emit next element in schedule
            Tuple2<Long, Object> head = emitSchedule.poll();
            long delayedEventTime = head.f0;

            long now = Calendar.getInstance().getTimeInMillis();
            long servingTime = toServingTime(serviceStartTime, dataStartTime, delayedEventTime);
            long waitTime = servingTime - now;

            Thread.sleep((waitTime > 0) ? waitTime : 0);

            if(head.f1 instanceof TaxiRide) {
                TaxiRide emitRide = (TaxiRide) head.f1;
                // emit ride
                try {
                    taxiRideEvent.write(emitRide);
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }
            } else if(head.f1 instanceof Watermark) {

            }
        }
    }

    private long toServingTime(long serviceStartTime, long dataStartTime, long eventTime) {
        long dataDiff = eventTime - dataStartTime;
        return serviceStartTime + (dataDiff / this.servingSpeed);
    }

    private long getNormalDelayMsecs(Random rand) {
        long delay = -1;
        long x = maxDelayMsecs / 2;
        while(delay < 0 || delay > maxDelayMsecs) {
            delay = (long)(rand.nextGaussian() * x) + x;
        }
        return delay;
    }

    private long getEventTime(TaxiRide ride) {
        if(ride.isStart) {
            return ride.startTime;
        } else {
            return ride.endTime;
        }
    }


    private static transient DateFormat dateFormat =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);


    private static TaxiRide taxiRideFromString(String line) {
        String[] tokens = line.split(",");
        if (tokens.length != 9) {
            throw new RuntimeException("Invalid record: " + line);
        }

        TaxiRide ride = new TaxiRide();

        try {
            ride.rideId = Long.parseLong((tokens[0]));

            switch (tokens[1]) {
                case "START":
                    ride.isStart = true;
                    ride.startTime = dateFormat.parse(tokens[2]).getTime();
                    ride.endTime = dateFormat.parse(tokens[3]).getTime();
                    break;
                case "END":
                    ride.isStart = false;
                    ride.endTime = dateFormat.parse(tokens[2]).getTime();
                    ride.startTime = dateFormat.parse(tokens[3]).getTime();
                    break;
                default:
                    throw new RuntimeException("Invalid record: " + line);
            }

            ride.startLon = tokens[4].length() > 0 ? Float.parseFloat(tokens[4]) : 0.0f;
            ride.startLat = tokens[5].length() > 0 ? Float.parseFloat(tokens[5]) : 0.0f;
            ride.endLon = tokens[6].length() > 0 ? Float.parseFloat(tokens[6]) : 0.0f;
            ride.endLat = tokens[7].length() > 0 ? Float.parseFloat(tokens[7]) : 0.0f;
            ride.passengerCnt = Short.parseShort(tokens[8]);
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }

        return ride;
    }

    public static void main(String[] args) throws org.apache.commons.cli.ParseException, InterruptedException {
        // Create Options object
        Options options = new Options();

        // add data file option
        options.addOption("data", true, "Path to the data file");

        // add the speed factor option
        options.addOption("speed", true, "The serving speed of the the data.");

        // Parse the command line arguments
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String dataFile = "";
        int speed = 0;

        if(cmd.hasOption("data") && cmd.hasOption("speed")){
            dataFile = cmd.getOptionValue("data");
            speed = Integer.parseInt(cmd.getOptionValue("speed"));
        } else {
            printUsage(options);
            System.exit(1);
        }

        TaxiRidePublisher publisher = new TaxiRidePublisher(dataFile, speed);

        Thread thread = new Thread(publisher);
        thread.run();
        thread.join();
    }

    private static void printUsage(Options options) {
        // automatically generate the help statement
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("taxiridepublisher", options);
    }
}
