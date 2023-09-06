package org.asouquieres.traveloptimizer.kstreams;

import com.michelin.kstreamplify.error.TopologyErrorHandler;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.asouquieres.traveloptimizer.kstreams.configuration.Constants;
import org.asouquieres.traveloptimizer.kstreams.topologies.models.TimeTableEntry;
import org.asouquieres.traveloptimizer.kstreams.topologies.processors.CustomerTravelRequestProcessor;
import org.asouquieres.traveloptimizer.kstreams.topologies.processors.DepartedProcessor;
import org.asouquieres.traveloptimizer.kstreams.topologies.processors.TimeTableUpdateProcessor;
import org.asouquieres.traveloptimizer.kstreams.topologies.serdes.JsonSerdesUtils;
import org.lboutros.traveloptimizer.model.*;
import org.springframework.stereotype.Component;

@Component
public class TravelOptimizerStream implements KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) {

        // Create state stores as usual
        final StoreBuilder<KeyValueStore<String, TimeTableEntry>> availableConnections =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(Constants.StateStores.AVAILABLE_CONNECTIONS_STATE_STORE),
                        Serdes.String(),
                        JsonSerdesUtils.getSerdesForValue()
                );

        final StoreBuilder<KeyValueStore<String, TravelAlert>> activeRequests =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(Constants.StateStores.LAST_REQUEST_ALERT_STATE_STORE),
                        Serdes.String(),
                        JsonSerdesUtils.getSerdesForValue()
                );

        final StoreBuilder<KeyValueStore<String, CustomerTravelRequest>> earlyRequests =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(Constants.StateStores.EARLY_REQUESTS_STATE_STORE),
                        Serdes.String(),
                        JsonSerdesUtils.getSerdesForValue()
                );

        streamsBuilder.addStateStore(availableConnections);
        streamsBuilder.addStateStore(activeRequests);
        streamsBuilder.addStateStore(earlyRequests);

        /*
        final KStream<String, CustomerTravelRequest> customerTravelRequestStream = streamsBuilder.stream(
                GlobalConstants.Topics.CUSTOMER_TRAVEL_REQUEST_TOPIC,
                Consumed.with(Serdes.String(), Constants.Serdes.CUSTOMER_TRAVEL_REQUEST_SERDE));

        // Get train time table stream
        final KStream<String, TrainTimeTableUpdate> trainTimeTableUpdateStream = streamsBuilder.stream(
                GlobalConstants.Topics.TRAIN_TIME_UPDATE_TOPIC,
                Consumed.with(Serdes.String(), Constants.Serdes.TRAIN_TIME_UPDATE_SERDE));

        // Get plane time table stream
        final KStream<String, PlaneTimeTableUpdate> planeTimeTableUpdateStream = streamsBuilder.stream(
                GlobalConstants.Topics.PLANE_TIME_UPDATE_TOPIC,
                Consumed.with(Serdes.String(), Constants.Serdes.PLANE_TIME_UPDATE_SERDE));

        // Get plane time table stream
        final KStream<String, Departure> departureStream = streamsBuilder.stream(
                GlobalConstants.Topics.DEPARTURE_TOPIC,
                Consumed.with(Serdes.String(), Constants.Serdes.DEPARTURE_SERDE));
         */


        // Get streams from centralized topic management class
        final KStream<String, CustomerTravelRequest> customerTravelRequestStream = TravelOptimizerTopics.customerRequests().stream(streamsBuilder);

        // Get train time table stream
        final KStream<String, TrainTimeTableUpdate> trainTimeTableUpdateStream = TravelOptimizerTopics.trainUpdate().stream(streamsBuilder);

        // Get plane time table stream
        final KStream<String, PlaneTimeTableUpdate> planeTimeTableUpdateStream = TravelOptimizerTopics.planeUpdate().stream(streamsBuilder);

        // Get plane time table stream
        final KStream<String, Departure> departureStream = TravelOptimizerTopics.departure().stream(streamsBuilder);


        // Now select key and repartition all streams
        KStream<String, CustomerTravelRequest> rekeyedCustomerTravelRequestStream = customerTravelRequestStream
                .selectKey((k, v) -> v.getDepartureLocation() + '#' + v.getArrivalLocation())
                .repartition(Repartitioned.with(Serdes.String(), JsonSerdesUtils.<CustomerTravelRequest>getSerdesForValue())
                        .withName(Constants.InternalTopics.REKEYED_CUSTOMER_TRAVEL_REQUEST_TOPIC));

        KStream<String, Departure> rekeyedDepartureStream = departureStream
                .selectKey((k, v) -> v.getDepartureLocation() + '#' + v.getArrivalLocation())
                .repartition(Repartitioned.with(Serdes.String(), JsonSerdesUtils.<Departure>getSerdesForValue())
                        .withName(Constants.InternalTopics.REKEYED_DEPARTURE_TOPIC));

        // We map train and plane time table update to generic time table updates in order to use only one state store for both streams
        KStream<String, TimeTableEntry> rekeyedTrainTimeTableUpdateStream = trainTimeTableUpdateStream
                .mapValues(TimeTableEntry::fromTrainTimeTableUpdate)
                .selectKey((k, v) -> v.getDepartureLocation() + '#' + v.getArrivalLocation())
                .repartition(Repartitioned.with(Serdes.String(), JsonSerdesUtils.<TimeTableEntry>getSerdesForValue())
                        .withName(Constants.InternalTopics.REKEYED_TRAIN_TIME_UPDATE_TOPIC));

        KStream<String, TimeTableEntry> rekeyedPlaneTimeTableUpdateStream = planeTimeTableUpdateStream
                .mapValues(TimeTableEntry::fromPlaneTimeTableUpdate)
                .selectKey((k, v) -> v.getDepartureLocation() + '#' + v.getArrivalLocation())
                .repartition(Repartitioned.with(Serdes.String(), JsonSerdesUtils.<TimeTableEntry>getSerdesForValue())
                        .withName(Constants.InternalTopics.REKEYED_PLANE_TIME_UPDATE_TOPIC));

        // Process each stream and produce travel alerts
        KStream<String, TravelAlert> trainTimeTableUpdateTravelAlertStream = rekeyedTrainTimeTableUpdateStream
                .process(TimeTableUpdateProcessor::new,
                        Constants.StateStores.LAST_REQUEST_ALERT_STATE_STORE,
                        Constants.StateStores.EARLY_REQUESTS_STATE_STORE,
                        Constants.StateStores.AVAILABLE_CONNECTIONS_STATE_STORE);

        KStream<String, TravelAlert> planeTimeTableUpdateTravelAlertStream = rekeyedPlaneTimeTableUpdateStream
                .process(TimeTableUpdateProcessor::new,
                        Constants.StateStores.LAST_REQUEST_ALERT_STATE_STORE,
                        Constants.StateStores.EARLY_REQUESTS_STATE_STORE,
                        Constants.StateStores.AVAILABLE_CONNECTIONS_STATE_STORE);

        KStream<String, TravelAlert> customerTravelRequestTravelAlertStream = rekeyedCustomerTravelRequestStream
                .process(CustomerTravelRequestProcessor::new,
                        Constants.StateStores.LAST_REQUEST_ALERT_STATE_STORE,
                        Constants.StateStores.EARLY_REQUESTS_STATE_STORE,
                        Constants.StateStores.AVAILABLE_CONNECTIONS_STATE_STORE);

        KStream<String, TravelAlert> departedTravelAlertStream = TopologyErrorHandler.catchErrors(rekeyedDepartureStream
                .process(DepartedProcessor::new,
                        Constants.StateStores.LAST_REQUEST_ALERT_STATE_STORE,
                        Constants.StateStores.AVAILABLE_CONNECTIONS_STATE_STORE));

        // Merge everything
        var mergedResult = trainTimeTableUpdateTravelAlertStream.merge(planeTimeTableUpdateTravelAlertStream)
                .merge(customerTravelRequestTravelAlertStream)
                .merge(departedTravelAlertStream);


        /*
                .to(GlobalConstants.Topics.TRAVEL_ALERTS_TOPIC,
                        Produced.with(Serdes.String(), Constants.Serdes.TRAVEL_ALERTS_SERDE));

         */

        // Produce using centralized topic management class
        TravelOptimizerTopics.travelAlert().produce(mergedResult);

    }

    @Override
    public String dlqTopic() {
        return TravelOptimizerTopics.dlq().toString();
    }
}
