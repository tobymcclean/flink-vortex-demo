package com.prismtech.vortex.flink.demo.subscriber;

import org.omg.dds.core.policy.Durability;
import vortex.commons.util.Idioms;
import vortex.flinkdemo.TaxiRide;

/**
 * Created by Vortex.
 */
public class TaxiRideSubscriber implements Runnable {

    @Override
    public void run() {
        Idioms.Event<TaxiRide> event = new Idioms.Event<TaxiRide>("TaxiRide", TaxiRide.class,
                Durability.Kind.TRANSIENT_LOCAL);

        event.observe(sample -> {
            if(sample.getData() != null) {
                printTaxiRide(sample.getData());
            }
        });
    }

    private void printTaxiRide(TaxiRide ride) {
        System.out.printf("%d\n", ride.rideId);
    }

    public static void main(String[] args) throws InterruptedException {
        TaxiRideSubscriber subscriber = new TaxiRideSubscriber();

        Thread thread = new Thread(subscriber);
        thread.run();

        thread.join();
    }
}
