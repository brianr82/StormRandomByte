package randombyte;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


public class RandomByteTopology {


    public static final String TOPOLOGY_NAME = "random-byte-experiment";



    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("generator",new RandomByteSpout(), 1).setMemoryLoad(12288)
                .addConfiguration("group-id", 11);

        builder.setBolt("relayer1",
                new RelayBolt(), 6)
                .shuffleGrouping("generator").setMemoryLoad(2048)
                .addConfiguration("group-id", 11);

        builder.setBolt("relayer2",
                new RelayBolt(), 6)
                .shuffleGrouping("relayer1").setMemoryLoad(2048)
                .addConfiguration("group-id", 24);



        builder.setBolt("relayer3",
                new RelayBolt(), 6)
                .shuffleGrouping("relayer2").setMemoryLoad(2048)
                .addConfiguration("group-id", 25);

        builder.setBolt("relayer4",
                new RelayBolt(), 6)
                .shuffleGrouping("relayer3").setMemoryLoad(2048)
                .addConfiguration("group-id", 26);



        builder.setBolt("appender", new AppendByteBolt(), 6)
                .shuffleGrouping("relayer4").setMemoryLoad(2048)
                .addConfiguration("group-id", 13);

        Config conf = new Config();
        //conf.setDebug(true);
        conf.setNumWorkers(2);
        conf.setTopologyWorkerMaxHeapSize(65536);




        StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME, conf, builder.createTopology());
        //Config topoConf = conf;


        //int runTime = 180;

        //  Submit topology to storm cluster
        //Helper.runOnClusterAndPrintMetrics(runTime, TOPOLOGY_NAME, topoConf, builder.createTopology());



    }
}