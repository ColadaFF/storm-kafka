package Topology;

import Cassandra.MessageStorageAppender;
import Cassandra.MessageStorageFactory;
import Message.EventMessageDeserializer;
import backtype.storm.ILocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.RawScheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;

public class TopologyBuilder {
    public static StormTopology buildLocal(ILocalDRPC drpc) {
        return build(drpc);
    }

    public static StormTopology buildRemote() {
        return build(null);
    }

    public static StormTopology build(ILocalDRPC drpc) {
        TridentTopology topology = new TridentTopology();
        Stream playStream = topology.newStream("play-stream", buildSpout())
                .each(
                        new Fields("bytes"),
                        new EventMessageDeserializer(),
                        new Fields("event")
                )
                .parallelismHint(2);

        TridentState saveEvents = playStream
                .project(new Fields("event"))
                .name("StorageCassandra")
                .partitionPersist(
                        new MessageStorageFactory(),
                        new Fields("event"),
                        new MessageStorageAppender()
                )
                .parallelismHint(4);

        return topology.build();
    }

    public static void main(String[] args) {
        TransactionalTridentKafkaSpout st = buildSpout();
        System.out.println(st.getOutputFields());
    }

    private static TransactionalTridentKafkaSpout buildSpout() {
        BrokerHosts zk = new ZkHosts(Properties.getString("kfm.storm.zkHost"));
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "play-stream");
        spoutConf.scheme = new SchemeAsMultiScheme(new RawScheme());
        return new TransactionalTridentKafkaSpout(spoutConf);
    }
}
