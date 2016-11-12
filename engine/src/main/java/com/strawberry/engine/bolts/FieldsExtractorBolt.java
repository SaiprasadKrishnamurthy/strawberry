package com.strawberry.engine.bolts;

import com.sai.strawberry.api.EventStreamConfig;
import com.strawberry.engine.config.StrawberryConfigHolder;
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
            Map raw = StrawberryConfigHolder.getJsonParser().readValue(tuple.getValue(0).toString(), Map.class);
            Map doc = (Map) raw.get("payload");
            EventStreamConfig eventStreamConfig = StrawberryConfigHolder.getJsonParser().convertValue(raw.get("eventStreamConfig"), EventStreamConfig.class);
            doc.put("__configId__", eventStreamConfig.getConfigId());
            doc.put("__naturalId__", doc.get(eventStreamConfig.getDocumentIdField()));
            System.out.println(boltId + " - Tuple Reached in the router bolt: " + doc);
            outputCollector.emit(tuple, new Values(doc,eventStreamConfig));
            outputCollector.ack(tuple);
        } catch (Exception ex) {
            outputCollector.reportError(ex);
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("doc", "eventStreamConfig"));
    }
}
