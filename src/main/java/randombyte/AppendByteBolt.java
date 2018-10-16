package randombyte;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.*;

import java.util.ArrayList;
import java.util.LinkedList;


public class AppendByteBolt extends BaseBasicBolt {

    int counter = 0;
    long window_start_time = 0;
    long start_time = 0;
    boolean written_to_file = false;


    private static final Logger LOG = LoggerFactory.getLogger(AppendByteBolt.class);

    LinkedList<WindowStatistics> stats_list = new LinkedList<WindowStatistics>();

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        if (window_start_time == 0) {
            window_start_time = System.nanoTime();
            start_time = System.nanoTime();
        }

        StringBuilder payload = new StringBuilder(100);
        for (int i=0; i<100; i++) {
            payload.append('P');
        }

        counter++;
        //basicOutputCollector.emit(new Values(payload.toString().getBytes()));
        long current_time = System.nanoTime();


        if (((current_time - window_start_time) / 1000000000l) > 1) {
            //System.out.println("\n*** Count *** " + counter + " ***\n");

            stats_list.add(new WindowStatistics(window_start_time, current_time, counter));
            window_start_time = current_time;
            counter = 0;


        }
        if((current_time - start_time) /1000000000l > 600 && written_to_file == false){
            LOG.info("###################################");
            for(WindowStatistics ws : stats_list){
                LOG.info(ws.getString());
            }

            LOG.info("###################################");
           for(WindowStatistics ws : stats_list){
               double throughput = (double)ws.count / ((ws.windowEndTime - ws.windowStartTime) / 1000000000l);
               long window_timestamp = ws.windowEndTime/1000000000l;
               LOG.info(window_timestamp +"," + throughput);
           }
        written_to_file = true;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //outputFieldsDeclarer.declare(new Fields("dataout"));
    }




}