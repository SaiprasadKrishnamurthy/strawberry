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
import org.springframework.data.mongodb.core.CollectionOptions;

import java.util.Map;

/**
 * Created by saipkri on 18/08/16.
 */
public class BatchSetupBolt extends BaseRichBolt {
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
            Map doc = (Map) tuple.getValueByField("doc");
            EventStreamConfig eventStreamConfig = (EventStreamConfig) tuple.getValueByField("eventStreamConfig");
            String topic = eventStreamConfig.getConfigId();

            if (eventStreamConfig.getBatchQueryConfig() != null) {
                if (!StrawberryConfigHolder.getMongoTemplateForBatch().collectionExists(topic)) {
                    CollectionOptions options = new CollectionOptions(eventStreamConfig.getBatchQueryConfig().getMaxBatchSizeInBytes(), eventStreamConfig.getBatchQueryConfig().getMaxNumberOfDocs(), true);
                    StrawberryConfigHolder.getMongoTemplateForBatch().createCollection(topic, options);
                }
                StrawberryConfigHolder.getMongoTemplateForBatch().save(doc, topic);
            } else {
                outputCollector.emit(tuple, new Values(doc, eventStreamConfig));
            }
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
