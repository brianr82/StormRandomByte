package randombyte;


import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;
import java.util.ArrayList;
import java.util.Map;


public class AppendByteBoltV2 extends BaseRichBolt {

    private OutputCollector _collector;
    private PrintWriter writer;

    // Statistics
    long currentRecordcount;
    long windowStartTime;
    long batch_size;


    int counter;
    int sequenceCounter;
    double windowMedianLatency;
    long window_total_latency;

    ArrayList<Long> window_latencies;
    long runningTotal;


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //outputFieldsDeclarer.declare(new Fields("dataout"));
    }


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        //create the log file
        String logPathDirectory = "/home/brianr/storm_metrics/";

        String filename =  logPathDirectory + "storm_consumer.txt";
        try {
            writer  = new PrintWriter(new FileOutputStream(new File(filename), false));
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        writer.println("---------[AppendByteBoltV2]--------------Supervisor Log -----------------------");


        currentRecordcount = 0l;
        windowStartTime  = System.nanoTime();
        batch_size = 1l;


        sequenceCounter = 0;
        windowMedianLatency =0;
        window_total_latency =0;

        window_latencies = new ArrayList<>();
        runningTotal =0l;


    }

    public void execute(Tuple tuple) {
        _collector.ack(tuple);
        String msgCreation = (String) tuple.getValue(1);
        counter++;
        runningTotal++;

        long new_message_produced_time = Long.parseLong(msgCreation);
        long new_message_latency = System.currentTimeMillis() - new_message_produced_time;

        window_latencies.add(new_message_latency);
        window_total_latency = window_total_latency + new_message_latency;


        /*****************Log the throughput******************************/
        long windowEndTime = System.nanoTime();
        long windowDuration = windowEndTime - windowStartTime;

        if (windowDuration > 1000000000l && currentRecordcount!=0) {


            //windowMedianLatency = computeWindowLatencyMedian(window_latencies);
            long windowAVGLatency = window_total_latency/currentRecordcount;

            writer.println("ConsumerID,SeqNumber,WindowDuration,Throughput,RunningTotal,MedianLatency,Total Latency,AVGLatency:" + "AppendBolt"  +" " + sequenceCounter  + " " + windowDuration +" " +  currentRecordcount +" " +  runningTotal + " " + windowMedianLatency + " "+ window_total_latency + " " + windowAVGLatency );
            System.out.println("AppendBolt" + " received this many tuples in one second: "+ currentRecordcount + " The running total is: " + runningTotal + " Duration: " + windowDuration);
            //reset the counter
            currentRecordcount = 0 ;
            windowMedianLatency =0;
            window_latencies.clear();
            window_total_latency =0;

            windowStartTime = windowEndTime ;
            sequenceCounter++;
            writer.flush();
        }
        else{
            currentRecordcount = currentRecordcount + batch_size ; //keep incrementing until the next window because 1 second has not yet passed since the last window closed
        }


    }
}