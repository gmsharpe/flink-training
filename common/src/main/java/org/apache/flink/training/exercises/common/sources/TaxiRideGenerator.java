/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.common.sources;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MappingIterator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.utils.DataGenerator;
import org.apache.flink.training.exercises.common.utils.DataLoader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This SourceFunction generates a data stream of TaxiRide records.
 *
 * <p>The stream is produced out-of-order.
 */
public class TaxiRideGenerator implements SourceFunction<TaxiRide> {

    public static final int SLEEP_MILLIS_PER_EVENT = 10;
    private static final int BATCH_SIZE = 5;
    private volatile boolean running = true;
    private final String sourceEventLogFile;

    public Map<Integer, Map<String, String>> locationsById;

    public static Map<Integer, Map<String, String>> loadLocationMap(String path) throws IOException {
        CsvMapper mapper = new CsvMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
        CsvSchema.Builder builder = CsvSchema.builder();
        // use first row as header otherwise defaults are fine
        CsvSchema schema = builder.setUseHeader(true).build();

        MappingIterator<Map<String, String>> it = mapper
                .readerFor(Map.class)
                .with(schema)
                .readValues(new File(path));

        List<Map<String, String>> locations = it.readAll();

        final List<Integer> ids = new ArrayList<>();

        // TODO - could convert these to integers in original python script for generating the location map
        Map<Integer, Map<String, String>> locationMap = locations.stream()
                .filter(l -> !ids.contains(new Double(l.get("location_i")).intValue()))
                .map(l ->
                    {
                        ids.add(new Double(l.get("location_i")).intValue());
                        return l;
                    })
                .collect(Collectors.toMap(
                        location -> new Double(location.get("location_i")).intValue(), // key mapper
                        location -> location)); //value mapper

        return locationMap;
    }

    Queue<DataLoader> taxiEventQueue;

    public TaxiRideGenerator(String sourceEventLogFile, String csvSourceData, String csvLocationMapData) throws IOException
    {
        taxiEventQueue = new LinkedList<>(DataLoader.readAllLines(csvSourceData));
        locationsById = loadLocationMap(csvLocationMapData);

        this.sourceEventLogFile = sourceEventLogFile;
        Path sourceEventLogPath = Paths.get(this.sourceEventLogFile);
        if(!Files.exists(sourceEventLogPath)) {
            Files.createDirectories(sourceEventLogPath.getParent());
            Files.createFile(sourceEventLogPath);
        }
        else {
            Files.delete(Paths.get(this.sourceEventLogFile));
            Files.createFile(sourceEventLogPath);
        }

        // append headers
        String headers = "rideId,isStart,eventTime,startLon,startLat,endLon," +
                         "endLat,passengerCnt,taxiId,driverId";

        Files.write(Paths.get(sourceEventLogFile),
                    (headers + '\n').getBytes(),
                    StandardOpenOption.APPEND);
    }

    @Override
    public void run(SourceContext<TaxiRide> ctx) throws Exception {

        PriorityQueue<TaxiRide> endEventQ = new PriorityQueue<>(100);
        long id = 0;
        long maxStartTime = 0;

        // generate a batch of START events
        List<TaxiRide> startEvents = new ArrayList<TaxiRide>(BATCH_SIZE);

        while(!taxiEventQueue.isEmpty()) {
            for (int i = 1; i <= BATCH_SIZE; i++) {

                DataLoader loader = taxiEventQueue.poll();
                TaxiRide ride = new TaxiRide(id + i, true, loader.setLatLongCoordinates(locationsById));


                startEvents.add(ride);
                // the start times may be in order, but let's not assume that
                maxStartTime = Math.max(maxStartTime, ride.getEventTimeMillis());

                endEventQ.add(new TaxiRide(id + i, false, loader.setLatLongCoordinates(locationsById)));
            }

            // release the END events coming before the end of this new batch
            // (this allows a few END events to precede their matching START event)
            while (endEventQ.peek().getEventTimeMillis() <= maxStartTime) {
                TaxiRide ride = endEventQ.poll();
                ctx.collect(ride);
                appendSourceEvent(ride);
            }

            // then emit the new START events (out-of-order)
            java.util.Collections.shuffle(startEvents, new Random(id));
            startEvents.iterator().forEachRemaining(ride -> {
                ctx.collect(ride);
                appendSourceEvent(ride);
            });

            // prepare for the next batch
            id += BATCH_SIZE;

            if(LocalDateTime.now().getMinute() % 5 == 0) {
                Thread.sleep(1000 * 35);
            }
            else {
                // don't go too fast
                Thread.sleep(BATCH_SIZE * SLEEP_MILLIS_PER_EVENT);
            }
        }
    }

    private void appendSourceEvent(TaxiRide ride)
    {
        try {
            Files.write(Paths.get(sourceEventLogFile),
                        (ride.toString() + '\n').getBytes(),
                        StandardOpenOption.APPEND);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
