package com.strawberry.engine.bolts;

import com.sai.strawberry.api.BatchQueryProcessor;
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
public class BatchQueryBolt extends BaseRichBolt {
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
            Map doc = (Map) tuple.getValue(0);
            if (StrawberryConfigHolder.getEventStreamConfig().getBatchQueryConfig() != null) {
                String currDoc = StrawberryConfigHolder.getJsonParser().writeValueAsString(doc);
                String jsonOut = invoke(StrawberryConfigHolder.getEventStreamConfig().getBatchQueryConfig().getBatchQueryProcessor(), currDoc);
                outputCollector.emit(tuple, new Values(jsonOut));
            } else {
                outputCollector.emit(tuple, new Values(doc));
            }
            outputCollector.ack(tuple);
        } catch (Exception ex) {
            outputCollector.reportError(ex);
        }
    }

    private String invoke(final String dataTransformer, final String jsonIn) throws Exception {
        Class<BatchQueryProcessor> clazz = (Class<BatchQueryProcessor>) Class.forName(dataTransformer);
        BatchQueryProcessor batchQueryProcessor = clazz.newInstance();
        return batchQueryProcessor.query(StrawberryConfigHolder.getMongoTemplateForBatch());
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("doc"));
    }
}
