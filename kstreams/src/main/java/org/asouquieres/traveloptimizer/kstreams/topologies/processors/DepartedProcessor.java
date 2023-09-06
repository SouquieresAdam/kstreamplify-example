package org.asouquieres.traveloptimizer.kstreams.topologies.processors;

import com.michelin.kstreamplify.error.ProcessingResult;
import org.apache.kafka.streams.processor.ConnectedStoreProvider;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.asouquieres.traveloptimizer.kstreams.configuration.Constants;
import org.asouquieres.traveloptimizer.kstreams.topologies.models.TimeTableEntry;
import org.lboutros.traveloptimizer.model.Departure;
import org.lboutros.traveloptimizer.model.TravelAlert;

import static org.asouquieres.traveloptimizer.kstreams.topologies.businessrules.EventManagement.whenADepartsOccurred;

public class DepartedProcessor extends ContextualProcessor<String, Departure, String, ProcessingResult<TravelAlert, Departure>> implements ConnectedStoreProvider {

    private KeyValueStore<String, TimeTableEntry> availableConnectionStateStore;
    private KeyValueStore<String, TravelAlert> lastRequestAlertStateStore;

    @Override
    public void init(ProcessorContext<String, ProcessingResult<TravelAlert, Departure>> context) {
        super.init(context);
        availableConnectionStateStore = context.getStateStore(Constants.StateStores.AVAILABLE_CONNECTIONS_STATE_STORE);
        lastRequestAlertStateStore = context.getStateStore(Constants.StateStores.LAST_REQUEST_ALERT_STATE_STORE);
    }

    @Override
    public void process(Record<String, Departure> record) {
        // This process is guaranteed to resist to poison pills
        try {
            whenADepartsOccurred(record.value(), lastRequestAlertStateStore, availableConnectionStateStore)
                    .forEach(travelAlert -> context()
                            .forward(ProcessingResult.wrapRecordSuccess(new Record<>(travelAlert.getId(), travelAlert, record.timestamp(), record.headers()))));
        } catch (Exception e) {
            context()
                    .forward(ProcessingResult.wrapRecordFailure(e, new Record<>(record.key(), record.value(), record.timestamp(), record.headers())));
        }

    }
}
