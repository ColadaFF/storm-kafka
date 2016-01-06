package Cassandra;

import Topology.Properties;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;

public class Connector {
    private Cluster cluster;

    public Cluster connect(String node) {
        cluster = Cluster.builder()
                .addContactPoint(node)
                .build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n",
                metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
        return cluster;
    }

    public Cluster getCluster(String node) {
        return Cluster.builder()
                .addContactPoint(node)
                .build();
    }

    public void close() {
        cluster.close();
    }

    public static void main(String[] args) {
        Connector ev = new Connector();
        ev.connect(Properties.getString("kfm.storm.csHost"));
        ev.close();
    }
}
