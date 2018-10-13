package randombyte;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class RandomByteTopology {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("generator",new RandomByteSpout(), 1).setMemoryLoad(12288)
                .addConfiguration("group-id", 1);

        builder.setBolt("relayer",
                new RelayBolt(), 1)
                .shuffleGrouping("generator").setMemoryLoad(2048)
                .addConfiguration("group-id", 1);

        builder.setBolt("appender", new AppendByteBolt(), 1)
                .shuffleGrouping("relayer").setMemoryLoad(2048)
                .addConfiguration("group-id", 2);

        Config conf = new Config();
        //conf.setDebug(true);
        conf.setNumWorkers(4);
        conf.setTopologyWorkerMaxHeapSize(16384);



        StormSubmitter.submitTopologyWithProgressBar("random-byte-experiment", conf, builder.createTopology());

    }
}