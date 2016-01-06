package Cassandra;

import Message.EventMessage;
import Message.EventMessageDeserializer;
import backtype.storm.tuple.Values;
import com.google.common.collect.Lists;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class MessageStorageAppender extends BaseStateUpdater<MessageStorage> {
    @Override
    public void updateState(MessageStorage messageStorage, List<TridentTuple> list, TridentCollector tridentCollector) {
        System.out.println("Called");
        List<EventMessage> eventMessages = new ArrayList<>();
        for (TridentTuple t : list) {
            EventMessage logLine = (EventMessage) t.getValue(0);
            eventMessages.add(logLine);
        }

        List<List<EventMessage>> operations = Lists.partition(eventMessages, 150);
        operations.forEach(messageStorage::saveBulkEvents);
    }
}
