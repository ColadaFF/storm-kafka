package Cassandra;

import Message.EventMessage;
import Topology.Properties;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import storm.trident.state.State;

import java.util.List;

public class MessageStorage implements State {
    private Cluster cluster = new Connector().connect(Properties.getString("kfm.storm.csHost"));
    private final Session session = cluster.connect("monitor");

    @Override
    public void beginCommit(Long aLong) {

    }

    @Override
    public void commit(Long aLong) {

    }

    public void saveBulkEvents(List<EventMessage> eventMessages) {
        String BeginQuery = "BEGIN BATCH ",
                EndQuery = " APPLY BATCH",
                Query = "";

        for (EventMessage eventMessage : eventMessages) {
            Query += " INSERT INTO events(machine, building, date, status, id) VALUES('" +
                    eventMessage.getMachine() + "','" +
                    eventMessage.getBuilding() + "'," +
                    eventMessage.getDate() + "," +
                    eventMessage.getStatus() + "," +
                    UUIDs.random() +
                    "); ";
        }
        System.out.println("Query" + Query);
        session.execute(BeginQuery + Query + EndQuery);
    }
}
