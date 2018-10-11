package randombyte;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class AppendByteBolt extends BaseBasicBolt {



    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String word = tuple.getStringByField("word");
        String append_word = word + "!!!";
        basicOutputCollector.emit(new Values(append_word));
        System.out.println("\n*** Appended Sentence Bolt *** " + append_word + " ***\n");

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("appended_word"));
    }
}