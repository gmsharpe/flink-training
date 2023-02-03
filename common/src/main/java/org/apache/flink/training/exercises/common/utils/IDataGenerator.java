package org.apache.flink.training.exercises.common.utils;

import java.time.Instant;

public interface IDataGenerator {
    Instant startTime();

    Instant endTime();

    long driverId();

    long taxiId();

    /*
        public float endLon() {
            return bFloat((float) (GeoUtils.LON_WEST - 0.1), (float) (GeoUtils.LON_EAST + 0.1F));
        }*/

    float startLat();

    float startLon();

    float endLat();

    float endLon();

    short passengerCnt();

    String paymentType();

    float tip();

    float tolls();

    float totalFare();

    long rideDurationMinutes();
}
