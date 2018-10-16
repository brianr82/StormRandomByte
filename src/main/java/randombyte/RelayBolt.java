package randombyte;


import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


import java.util.Map;

public class RelayBolt extends BaseRichBolt {

    /*public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {


        StringBuilder payload = new StringBuilder(100);
        for (int i=0; i<100; i++) {
            payload.append('P');
        }
        basicOutputCollector.emit(new Values(payload.toString().getBytes()),);

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("dataout"));
    }

    */


    private OutputCollector _collector;


    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }


    public void execute(Tuple input) {


        StringBuilder payload = new StringBuilder(100);
        for (int i=0; i<100; i++) {
            payload.append('P');
        }

        _collector.emit(input, new Values(payload.toString().getBytes()));
        _collector.ack(input);
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("dataout"));
    }


}