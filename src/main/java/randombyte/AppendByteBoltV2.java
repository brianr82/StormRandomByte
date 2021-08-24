package randombyte;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.io.FileWriter;
import java.util.Map;
import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;

public class AppendByteBoltV2 extends BaseRichBolt {

    private OutputCollector _collector;
    private FileWriter csvWriter;

    private static final Logger LOG = LoggerFactory.getLogger(AppendByteBoltV2.class);


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //outputFieldsDeclarer.declare(new Fields("dataout"));
    }


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;

        //create the log file
        try {
            this.csvWriter = new FileWriter(new File("/home/brianr/storm_metrics/storm_" + System.currentTimeMillis() + ".csv"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void execute(Tuple tuple) {


        String msgCreation = (String) tuple.getValue(1);
        Long endtoend = Duration.between(ZonedDateTime.parse(msgCreation), ZonedDateTime.now()).toNanos();

        try {
            this.csvWriter.append(System.currentTimeMillis() + ";" + endtoend + "\n");
            this.csvWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }



        _collector.ack(tuple);
    }
}