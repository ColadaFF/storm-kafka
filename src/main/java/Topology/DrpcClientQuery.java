package Topology;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import org.apache.thrift7.TException;

public class DrpcClientQuery {
    public static void main(String[] args) {
        DRPCClient client = new DRPCClient("localhost", 3772);
        try {
            String result = client.execute("count-request-by-machine", "pump_1,pump_2,tank_1,tank_2");
            System.out.println(result);
        } catch (TException | DRPCExecutionException e) {
            e.printStackTrace();
        }
    }
}
