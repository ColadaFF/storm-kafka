package Storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class StreamTopology {
    private final Logger LOGGER = Logger.getLogger(this.getClass());
    private static final String KAFKA_TOPIC = Properties.getString("kfm.storm.kafka_topic");

    public static void main(String[] args) throws InterruptedException {
        BasicConfigurator.configure();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(
                "kafka-events-streams",
                createConfig(true),
                createTopology()
        );
        Thread.sleep(60000);
        cluster.shutdown();

    }

    private static boolean shouldRunInDebugMode(String[] args) {
        return args.length > 1 && "debug".equalsIgnoreCase(args[1]);
    }


    private static StormTopology createTopology() {
        SpoutConfig kafkaConf = new SpoutConfig(
                new ZkHosts(Properties.getString("kfm.storm.zkHost")),
                KAFKA_TOPIC,
                "/kafka",
                "KafkaSpout"
        );
        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        TopologyBuilder builder = new TopologyBuilder();

        builder
                .setSpout("kafka_spout", new KafkaSpout(kafkaConf), 4);

        builder
                .setBolt("kafka_logger", new KafkaLogger(), 4)
                .shuffleGrouping("kafka_spout");

        return builder.createTopology();
    }

    private static Config createConfig(boolean local) {
        int workers = Properties.getInt("kfm.storm.workers");
        Config conf = new Config();
        conf.setDebug(false);
        if (local) {
            conf.setMaxTaskParallelism(workers);
        } else {
            conf.setNumWorkers(workers);
        }
        return conf;
    }
}
