package org.asouquieres.traveloptimizer.kstreams.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public interface Constants {

    ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    interface StateStores {
        String AVAILABLE_CONNECTIONS_STATE_STORE = "availableConnectionStateStore";
        String LAST_REQUEST_ALERT_STATE_STORE = "lastRequestAlertStateStore";
        String EARLY_REQUESTS_STATE_STORE = "earlyRequestStateStore";
    }

    interface InternalTopics {
        String REKEYED_CUSTOMER_TRAVEL_REQUEST_TOPIC = "rekeyedCustomerTravelRequested";
        String REKEYED_PLANE_TIME_UPDATE_TOPIC = "rekeyedPlaneTimeUpdated";
        String REKEYED_TRAIN_TIME_UPDATE_TOPIC = "rekeyedTrainTimeUpdated";
        String REKEYED_DEPARTURE_TOPIC = "rekeyedDeparted";
    }
}
