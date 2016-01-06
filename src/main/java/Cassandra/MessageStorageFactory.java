package Cassandra;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

public class MessageStorageFactory implements StateFactory {
    @Override
    public State makeState(Map map, IMetricsContext iMetricsContext, int i, int i1) {
        return new MessageStorage();
    }
}
