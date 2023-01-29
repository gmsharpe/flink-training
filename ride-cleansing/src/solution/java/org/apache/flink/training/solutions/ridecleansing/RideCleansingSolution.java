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

package org.apache.flink.training.solutions.ridecleansing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvFactoryBuilder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.impl.CsvEncoder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.GeoUtils;

import java.time.Duration;
import java.util.function.Function;

/**
 * Solution to the Ride Cleansing exercise from the Flink training.
 *
 * <p>The task of this exercise is to filter a data stream of taxi ride records to keep only rides
 * that both start and end within New York City. The resulting stream should be printed.
 */
public class RideCleansingSolution {

    private final SourceFunction<TaxiRide> source;
    private final Sink<TaxiRide> sink;


    /** Creates a job using the source and sink provided. */
    public RideCleansingSolution(SourceFunction<TaxiRide> source,  Sink<TaxiRide> sink) {

        this.source = source;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception
    {
        FileSink fileSink =
                FileSink.forRowFormat(new Path(args[0]),
                                               new SimpleStringEncoder<String>("UTF-8"))
                                 .withRollingPolicy(DefaultRollingPolicy.builder()
                                                                      //  .withRolloverInterval(Duration.ofMinutes(1))
                                                                      //  .withInactivityInterval(Duration.ofSeconds(30))
                                                                      //  .withMaxPartSize(MemorySize.ofMebiBytes(1))
                                                                        .build())
                                 .build();

        RideCleansingSolution job =
                new RideCleansingSolution(new TaxiRideGenerator(), fileSink);

        job.execute();
    }

    /**
     * Creates and executes the long rides pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the pipeline
        env.addSource(source).filter(new NYCFilter()).sinkTo(sink);

        // run the pipeline and return the result
        return env.execute("Taxi Ride Cleansing");
    }

    /** Keep only those rides and both start and end in NYC. */
    public static class NYCFilter implements FilterFunction<TaxiRide> {
        @Override
        public boolean filter(TaxiRide taxiRide) {
            return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat)
                    && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
        }
    }
}
