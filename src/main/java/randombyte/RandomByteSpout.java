package randombyte;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.util.UUID;


public class RandomByteSpout extends BaseRichSpout {



    private SpoutOutputCollector collector;


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

    }

    public void nextTuple() {
        StringBuilder payload = new StringBuilder(100);
        for (int i=0; i<100; i++) {
            payload.append('P');
        }

        UUID msgID = UUID.randomUUID();
        this.collector.emit(new Values(payload.toString().getBytes()),msgID);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("dataout"));
    }


    public void ack(Object msgId){


    }

}