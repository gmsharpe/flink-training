package org.apache.flink.training.exercises.common.utils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MappingIterator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.*;
import java.util.*;
import java.util.function.BiFunction;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DataLoader implements IDataGenerator, Serializable
{
    /**
     * 'VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime',
     * 'store_and_fwd_flag', 'RatecodeID', 'PULocationID', 'DOLocationID',
     * 'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax',
     * 'tip_amount', 'tolls_amount', 'ehail_fee', 'improvement_surcharge',
     * 'total_amount', 'payment_type', 'trip_type', 'congestion_surcharge'
     */

    public static List<DataLoader> readAllLines(String csvSourceData) throws IOException {
        CsvMapper mapper = new CsvMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
        CsvSchema.Builder builder = CsvSchema.builder();
        // use first row as header otherwise defaults are fine
        CsvSchema schema = builder.setUseHeader(true).build();

        MappingIterator<DataLoader> it = mapper
                .readerFor(DataLoader.class)
                .with(schema)
                .readValues(new File(csvSourceData));

        return it.readAll();
    }

    public static List<String> headers =   Arrays.asList(new String[]{"Idx", "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime",
        "store_and_fwd_flag", "RatecodeID", "PULocationID", "DOLocationID",
        "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "ehail_fee", "improvement_surcharge",
        "total_amount", "payment_type", "trip_type", "congestion_surcharge"});


    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
    @JsonProperty("lpep_pickup_datetime")
    LocalDateTime startTimeLocalDateTime;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
    @JsonProperty("lpep_dropoff_datetime")
    LocalDateTime endTimeLocalDateTime;

    long driverId;
    @JsonProperty("VendorID")
    long taxiId;

    @JsonProperty("PULocationID")
    int pULocationId;
    @JsonProperty("DOLocationID")
    int dOLocationId;

    float startLat;
    float startLon;
    float endLat;
    float endLon;
    @JsonProperty("passenger_count")
    String passengerCnt;

    @JsonProperty("payment_type")
    String paymentType;
    @JsonProperty("tip_amount")
    float tip;
    @JsonProperty("tolls_amount")
    float tolls;
    @JsonProperty("total_amount")
    float totalFare;

    @Override
    public Instant startTime() {
        return this.startTimeLocalDateTime.toInstant(OffsetDateTime.now().getOffset());
    }

    @Override
    public Instant endTime() {
        return this.endTimeLocalDateTime.toInstant(OffsetDateTime.now().getOffset());
    }

    @Override
    public long driverId() {
        return this.driverId;
    }

    @Override
    public long taxiId() {
        return this.taxiId;
    }

    @Override
    public float startLat() {
        return this.startLat;
    }

    @Override
    public float startLon() {
        return this.startLon;
    }

    @Override
    public float endLat() {
        return this.endLat;
    }

    @Override
    public float endLon() {
        return this.endLon;
    }

    @Override
    public short passengerCnt() {
        return new Double(this.passengerCnt).shortValue();
    }

    @Override
    public String paymentType() {
        return this.paymentType;
    }

    @Override
    public float tip() {
        return this.tip;
    }

    @Override
    public float tolls() {
        return this.tolls;
    }

    @Override
    public float totalFare() {
        return this.totalFare;
    }

    @Override
    public long rideDurationMinutes() {
        return Duration.between(this.startTimeLocalDateTime, this.endTimeLocalDateTime).toMinutes();
    }

    @Override
    public int pULocationId() {
        return this.pULocationId;
    }

    @Override
    public int dOLocationId() {
        return this.dOLocationId;
    }

    public DataLoader setLatLongCoordinates(Map<Integer, Map<String, String>> locationsById) {
        if(locationsById.containsKey(this.pULocationId)) {
            this.startLat = Float.parseFloat(locationsById.get(this.pULocationId).get("latitude"));
            this.startLon = Float.parseFloat(locationsById.get(this.pULocationId).get("longitude"));
        }
        else {

        }
        if(locationsById.containsKey(this.dOLocationId)) {
            this.endLat = Float.parseFloat(locationsById.get(this.dOLocationId).get("latitude"));
            this.endLon = Float.parseFloat(locationsById.get(this.dOLocationId).get("longitude"));
        }
        else {

        }

        return this;

    }
}
