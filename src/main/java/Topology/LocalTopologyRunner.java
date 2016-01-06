package Topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.utils.Utils;

public class LocalTopologyRunner {
    private static final int TWO_MINUTES = 120000;

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setDebug(false);

        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafka-events-processor", config, TopologyBuilder.buildLocal(drpc));

        Utils.sleep(TWO_MINUTES);
        System.out.println("DONE _________________________________");

        /*String result = drpc.execute("count-request-by-machine", "pump_1,pump_2,tank_1,tank_2");
        System.out.println("RESULTS");
        System.out.println("==========================================================================");
        System.out.println(result);
        System.out.println("==========================================================================");
*/
        cluster.killTopology("kafka-events-processor");
        cluster.shutdown();
        drpc.shutdown();
    }
}
