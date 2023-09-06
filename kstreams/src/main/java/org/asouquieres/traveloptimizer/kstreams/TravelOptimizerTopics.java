package org.asouquieres.traveloptimizer.kstreams;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.utils.SerdesUtils;
import com.michelin.kstreamplify.utils.TopicWithSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.asouquieres.traveloptimizer.kstreams.topologies.serdes.JsonSerdesUtils;
import org.lboutros.traveloptimizer.GlobalConstants;
import org.lboutros.traveloptimizer.model.*;

public final class TravelOptimizerTopics {

    private TravelOptimizerTopics() {
    }

    // DLQ
    public static TopicWithSerde<String, KafkaError> dlq() {
        return new TopicWithSerde<>("dlqTopic", Serdes.String(), SerdesUtils.getSerdesForValue());
    }

    public static TopicWithSerde<String, CustomerTravelRequest> customerRequests() {
        return new TopicWithSerde<>(GlobalConstants.Topics.CUSTOMER_TRAVEL_REQUEST_TOPIC, Serdes.String(), JsonSerdesUtils.getSerdesForValue());
    }

    public static TopicWithSerde<String, TravelAlert> travelAlert() {
        return new TopicWithSerde<>(GlobalConstants.Topics.TRAVEL_ALERTS_TOPIC, Serdes.String(), JsonSerdesUtils.getSerdesForValue());
    }

    public static TopicWithSerde<String, PlaneTimeTableUpdate> planeUpdate() {
        return new TopicWithSerde<>(GlobalConstants.Topics.PLANE_TIME_UPDATE_TOPIC, Serdes.String(), JsonSerdesUtils.getSerdesForValue());
    }

    public static TopicWithSerde<String, TrainTimeTableUpdate> trainUpdate() {
        return new TopicWithSerde<>(GlobalConstants.Topics.TRAIN_TIME_UPDATE_TOPIC, Serdes.String(), JsonSerdesUtils.getSerdesForValue());
    }

    public static TopicWithSerde<String, Departure> departure() {
        return new TopicWithSerde<>(GlobalConstants.Topics.DEPARTURE_TOPIC, Serdes.String(), JsonSerdesUtils.getSerdesForValue());
    }

}
