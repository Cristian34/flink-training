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

package org.apache.flink.training.exercises.longrides;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * The "Long Ride Alerts" exercise.
 *
 * <p>The goal for this exercise is to emit the rideIds for taxi rides with a duration of more than
 * two hours. You should assume that TaxiRide events can be lost, but there are no duplicates.
 *
 * <p>You should eventually clear any state you create.
 */
public class LongRidesExercise {

    private static final long MAX_DURATION_MS = 2 * 3600 * 1000;
    private static final long MAX_EVENT_DELAY_SECONDS = 60;
    // define a safety margin to add to timers to protect against time race conditions
    private static final long SAFETY_MARGIN_MS = 60 * 1000;

    private final SourceFunction<TaxiRide> source;
    private final SinkFunction<Long> sink;

    /** Creates a job using the source and sink provided. */
    public LongRidesExercise(SourceFunction<TaxiRide> source, SinkFunction<Long> sink) {
        this.source = source;
        this.sink = sink;
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

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(source);

        // the WatermarkStrategy specifies how to extract timestamps and generate watermarks
        WatermarkStrategy<TaxiRide> watermarkStrategy =
                WatermarkStrategy.<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(MAX_EVENT_DELAY_SECONDS))
                        .withTimestampAssigner(
                                (ride, streamRecordTimestamp) -> ride.getEventTimeMillis());

        // create the pipeline
        rides.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(ride -> ride.rideId)
                .process(new AlertFunction())
                .addSink(sink);

        // execute the pipeline and return the result
        return env.execute("Long Taxi Rides");
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        LongRidesExercise job =
                new LongRidesExercise(new TaxiRideGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    public static class AlertFunction extends KeyedProcessFunction<Long, TaxiRide, Long> {

        // register the ride START or END event (whichever comes first)
        // this implementation assumes that there are no event duplicates
        // (we can have at maximum a START and an END event)
        private transient ValueState<TaxiRide> registeredRide;
        // store the timer timestamp for cleaning idle state (if the corresponding event doesn't arrive)
        private transient ValueState<Long> idleStateTimer;

        @Override
        public void open(Configuration config) {
            registeredRide = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("registeredEvent", TaxiRide.class));
            idleStateTimer = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("idleStateTimer", Long.class));
        }

        @Override
        public void processElement(TaxiRide ride, Context ctx, Collector<Long> out) throws Exception {

            TimerService timerService = ctx.timerService();
            if (ride.getEventTimeMillis() < timerService.currentWatermark()) {
                return; // ignore late event
            }
            if (ride.isStart) {
                if (registeredRide.value() != null) { // if END event already registered
                    if (isLongDuration(ride.getEventTimeMillis(), registeredRide.value().getEventTimeMillis())) {
                        out.collect(ride.rideId);
                        registeredRide.clear();
                    }
                    // delete "idle state" timer in case it has been set
                    if (idleStateTimer.value() != null) {
                        timerService.deleteEventTimeTimer(idleStateTimer.value());
                        idleStateTimer.clear();
                    }
                } else { // if no END event registered yet, register START event and timer
                    registeredRide.update(ride);
                    // register "long duration" timer
                    timerService.registerEventTimeTimer(ride.getEventTimeMillis() + MAX_DURATION_MS);
                }
            } else {
                if (registeredRide.value() != null) { // START event already registered
                    if (isLongDuration(registeredRide.value().getEventTimeMillis(), ride.getEventTimeMillis())) {
                        // if we have a long ride but timer not launched yet (maybe because of a time race condition)
                        out.collect(ride.rideId);
                    }
                    timerService.deleteEventTimeTimer(registeredRide.value().getEventTimeMillis() + MAX_DURATION_MS);
                    registeredRide.clear();
                } else {
                    // if END event comes before the start event or the START event was dismissed (being late),
                    // we don't set a timer, since it will be logically strange (time should go forward); in this case,
                    // long ride detection will be done through a regular check
                    registeredRide.update(ride);
                    // register "idle state" expiration timer for the situations:
                    //  - the START event is late and not processed
                    //  - the END event comes after the "long ride" timer fires
                    // Obs: we don't need to use the "idle state" expiration timer for START events since state
                    // will be clean up by the "long ride" timer if the END event is missing
                    idleStateTimer.update(ride.getEventTimeMillis() + MAX_EVENT_DELAY_SECONDS * 1000
                            + SAFETY_MARGIN_MS);
                    timerService.registerEventTimeTimer(idleStateTimer.value());
                }
            }
        }

        public void onTimer(long timestamp, OnTimerContext context, Collector<Long> out)
                throws Exception {

            if (idleStateTimer.value() != null) {
                // "idle state" timer
                idleStateTimer.clear();
            } else {
                // "long ride" timer
                out.collect(registeredRide.value().rideId);
            }
            registeredRide.clear();
        }

        private boolean isLongDuration(long startTimestamp, long endTimestamp) {
            return endTimestamp - startTimestamp > MAX_DURATION_MS;
        }

    }

}
