package randombyte;

import model.IDGenerator;
import model.TupleHeader;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


import java.util.Map;
import java.util.UUID;
import java.time.ZonedDateTime;

public class RandomByteSpout extends BaseRichSpout {



    private SpoutOutputCollector collector;


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

    }

    public void nextTuple() {
        int size = 50;
        StringBuilder payload = new StringBuilder(size);
        for (int i=0; i<size; i++) {
            payload.append('A');
        }


        model.Tuple tuple = new model.Tuple();
        UUID tupleID = IDGenerator.generateType1UUID();

        long currentTime = System.currentTimeMillis();

        TupleHeader tupleHeader = new TupleHeader(tupleID.toString(), String.valueOf(currentTime), String.valueOf(currentTime), "", "");
        tuple.setTupleHeader(tupleHeader);


        this.collector.emit(new Values(payload.toString().getBytes(),tuple.getTupleHeader().getTimeStampFromSource()));


      /*  try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("dataout"));
    }


    public void ack(Object msgId){


    }

}