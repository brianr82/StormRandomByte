package randombyte;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class AppendByteBolt extends BaseBasicBolt {



    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        StringBuilder payload = new StringBuilder(100);
        for (int i=0; i<100; i++) {
            payload.append('P');
        }

        basicOutputCollector.emit(new Values(payload.toString().getBytes()));

        System.out.println("\n*** Appended Sentence Bolt *** " + payload + " ***\n");

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("dataout"));
    }
}