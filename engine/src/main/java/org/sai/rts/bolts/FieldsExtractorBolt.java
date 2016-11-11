package org.sai.rts.bolts;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by saipkri on 18/08/16.
 */
public class FieldsExtractorBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private int boltId;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.boltId = topologyContext.getThisTaskId();
    }

    @Override
    public void execute(final Tuple tuple) {
        try {
            Map doc = new ObjectMapper().readValue(tuple.getValue(0).toString(), Map.class);
            String documentPartitionKey = doc.get("partitionKey").toString();
            String topic = doc.get("topicName").toString();
            System.out.println(boltId + " - Tuple Reached in the router bolt: " + doc + " --> " + documentPartitionKey);
            outputCollector.emit(tuple, new Values(documentPartitionKey, topic, tuple.getString(0)));
            outputCollector.ack(tuple);
        } catch (Exception ex) {
            ex.printStackTrace();
            outputCollector.reportError(ex);
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("partitionKey", "topic", "doc"));
    }
}
